#define _GNU_SOURCE
#include "lb_model.h"

#include <cjson/cJSON.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

// LightGBM C API (lib_lightgbm). 헤더 없이 쓰는 함수만 선언한다
typedef void *BoosterHandle;
int LGBM_BoosterCreateFromModelfile(const char *filename, int *out_num_iterations, BoosterHandle *out);
int LGBM_BoosterGetNumFeature(BoosterHandle handle, int *out_len);
int LGBM_BoosterPredictForMat(BoosterHandle handle, const void *data, int data_type, int32_t nrow, int32_t ncol,
                              int is_row_major, int predict_type, int start_iteration, int num_iteration,
                              const char *parameter, int64_t *out_len, double *out_result);
int LGBM_BoosterFree(BoosterHandle handle);
const char *LGBM_GetLastError(void);
#define C_API_DTYPE_FLOAT64 1
#define C_API_PREDICT_NORMAL 0

#define RING_SEC 64            // 2의 거듭제곱. 필요한 과거(최대 45초)보다 길게
#define V_LOOKBACK 45          // viewer_count ffill을 찾는 길이 (초)
#define STALE_SEC 2            // 이 초 수보다 오래 새 행이 없으면 표에서 뺀다
#define HIST_SEC 11            // pending 계산: t−10..t의 누적 배정 수
#define PER_VIEWER_CORE 0.01
#define WAKE_AFTER_NS 50000000 // 초가 끝나고 50 ms 뒤에 계산
#define K_ROWS (N2SL_KMAX + 2)
#define MAX_FEATS 8
#define SIMPLE_TIER2 1000.0  // simple: 자리가 없고 throttle도 없는 서버 비용 = 1000 + 예상 이용률
#define SIMPLE_TIER3 2000.0  // simple: throttle이 있는 서버 비용 = 2000 + 예상 throttle(%)

enum { F_UTIL, F_PEAK, F_INC, F_PROJ, F_DUTIL10, F_PAST10, F_PAST30, F_EFF };

typedef struct {
    int64_t sec;               // 이 칸의 초. 다른 초면 비어 있는 칸
    double cpu_sum, cpu_max, thr_us, vc;
    int cpu_n, has_vc;
} SecBin;

struct LbPod {
    pthread_mutex_t mtx;
    int active;                // 0 = 사라진 pod의 칸 (표에는 key만 남고 입력 없음). pods_mtx로 보호
    int fresh;                 // 1 = 막 (다시) 쓰기 시작한 칸 → 다음 계산에서 pending 기록을 지금 값으로 맞춤
    char key[N2SL_KEY_LEN];
    int has_first;
    int64_t first_sec, last_sec;
    double cpu_limit;
    SecBin bin[RING_SEC];
};

typedef struct {
    BoosterHandle h;
    int feats[MAX_FEATS];
    int n_feats;
    double platt_a, platt_b;   // sev는 쓰지 않음
} Model;

struct LbModel {
    LbModelConfig cfg;
    Model warn, crit, sev;
    double eff_cap;
    // pod 칸을 추가/비우기(main의 sync)와 표 계산(모델 스레드)이 겹치지 않게 한다.
    // n_pods는 줄지 않는다 (칸 번호 = 표 번호 = assign 번호라서 칸을 당기면 누적 배정 수가 다른 pod로 옮겨감)
    pthread_mutex_t pods_mtx;
    int n_pods;
    LbPod pods[N2SL_MAX_SRV];

    n2sl_table_t *table;
    n2sl_assign_t *assign;
    FILE *log;

    uint32_t hist[HIST_SEC][N2SL_MAX_SRV];
    int hist_n, hist_pos;

    pthread_t thread;
    atomic_int running;
    int thread_started;

    double xrow[N2SL_MAX_SRV * K_ROWS * MAX_FEATS];
    double out_warn[N2SL_MAX_SRV * K_ROWS];
    double out_sev[N2SL_MAX_SRV * K_ROWS];
    double out_crit[N2SL_MAX_SRV];
    LbResult res;
};

// ---- 설정 ----

static void trim(char *s) {
    size_t n = strlen(s);
    while (n > 0 && strchr(" \t\r\n", s[n - 1]))
        s[--n] = '\0';
    size_t i = strspn(s, " \t");
    if (i > 0)
        memmove(s, s + i, n - i + 1);
}

static void copy_str(char *dst, size_t size, const char *src) {
    snprintf(dst, size, "%s", src);
}

int lb_model_config_load(const char *path, LbModelConfig *out) {
    memset(out, 0, sizeof(*out));
    copy_str(out->table_shm, sizeof(out->table_shm), "/n2sl_table");
    copy_str(out->assign_shm, sizeof(out->assign_shm), "/n2sl_assign");
    out->p_th = 0.10;
    out->objective = LB_OBJECTIVE_SERVER;
    out->num_threads = 2;
    out->sat_pct = 90;
    out->d_default = 1;

    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "lb_model: 설정 파일 열기 실패 %s: %s\n", path, strerror(errno));
        return -1;
    }
    char line[512];
    int bad = 0;
    while (fgets(line, sizeof(line), f)) {
        trim(line);
        char *eq = strchr(line, '=');
        if (line[0] == '\0' || line[0] == '#' || !eq)
            continue;
        *eq = '\0';
        char *key = line, *val = eq + 1;
        trim(key);
        trim(val);
        if (!strcmp(key, "model_dir"))
            copy_str(out->model_dir, sizeof(out->model_dir), val);
        else if (!strcmp(key, "table_shm"))
            copy_str(out->table_shm, sizeof(out->table_shm), val);
        else if (!strcmp(key, "assign_shm"))
            copy_str(out->assign_shm, sizeof(out->assign_shm), val);
        else if (!strcmp(key, "log_path"))
            copy_str(out->log_path, sizeof(out->log_path), val);
        else if (!strcmp(key, "p_th"))
            out->p_th = atof(val);
        else if (!strcmp(key, "num_threads"))
            out->num_threads = atoi(val);
        else if (!strcmp(key, "sat_pct"))
            out->sat_pct = atof(val);
        else if (!strcmp(key, "d_default"))
            out->d_default = atof(val);
        else if (!strcmp(key, "objective")) {
            if (!strcmp(val, "server"))
                out->objective = LB_OBJECTIVE_SERVER;
            else if (!strcmp(val, "people"))
                out->objective = LB_OBJECTIVE_PEOPLE;
            else if (!strcmp(val, "simple"))
                out->objective = LB_OBJECTIVE_SIMPLE;
            else
                bad = 1;
        } else {
            fprintf(stderr, "lb_model: 모르는 설정 %s\n", key);
            bad = 1;
        }
    }
    fclose(f);
    if ((out->objective != LB_OBJECTIVE_SIMPLE && out->model_dir[0] == '\0') || out->p_th <= 0 || out->p_th >= 1 ||
        out->num_threads < 1 || out->sat_pct <= 0 || out->sat_pct > 100 || out->d_default <= 0)
        bad = 1;
    if (bad)
        fprintf(stderr, "lb_model: 설정 값 오류 %s\n", path);
    return bad ? -1 : 0;
}

// ---- 모델 읽기 ----

static int feat_id(const char *name) {
    static const char *names[] = {"util", "peak", "inc", "proj", "dutil10", "past10", "past30", "eff"};
    for (int i = 0; i < MAX_FEATS; i++)
        if (!strcmp(name, names[i]))
            return i;
    return -1;
}

static char *read_file(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f)
        return NULL;
    fseek(f, 0, SEEK_END);
    long n = ftell(f);
    rewind(f);
    char *s = malloc((size_t)n + 1);
    if (s && fread(s, 1, (size_t)n, f) != (size_t)n) {
        free(s);
        s = NULL;
    }
    if (s)
        s[n] = '\0';
    fclose(f);
    return s;
}

static int load_model(Model *md, const cJSON *models, const char *name, const char *dir, int need_platt) {
    const cJSON *j = cJSON_GetObjectItem(models, name);
    const cJSON *file = j ? cJSON_GetObjectItem(j, "file") : NULL;
    const cJSON *feats = j ? cJSON_GetObjectItem(j, "features") : NULL;
    if (!cJSON_IsString(file) || !cJSON_IsArray(feats)) {
        fprintf(stderr, "lb_model: meta.json에 %s 정보가 없음\n", name);
        return -1;
    }
    md->n_feats = cJSON_GetArraySize(feats);
    if (md->n_feats > MAX_FEATS)
        return -1;
    for (int i = 0; i < md->n_feats; i++) {
        const cJSON *c = cJSON_GetArrayItem(feats, i);
        md->feats[i] = cJSON_IsString(c) ? feat_id(c->valuestring) : -1;
        if (md->feats[i] < 0) {
            fprintf(stderr, "lb_model: %s의 입력을 모름 (%d번째)\n", name, i);
            return -1;
        }
    }
    if (need_platt) {
        const cJSON *a = cJSON_GetObjectItem(j, "platt_a"), *b = cJSON_GetObjectItem(j, "platt_b");
        if (!cJSON_IsNumber(a) || !cJSON_IsNumber(b)) {
            fprintf(stderr, "lb_model: %s 보정 값이 없음\n", name);
            return -1;
        }
        md->platt_a = a->valuedouble;
        md->platt_b = b->valuedouble;
    }
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", dir, file->valuestring);
    int iters = 0, nf = 0;
    if (LGBM_BoosterCreateFromModelfile(path, &iters, &md->h) != 0) {
        fprintf(stderr, "lb_model: %s 읽기 실패: %s\n", path, LGBM_GetLastError());
        return -1;
    }
    if (LGBM_BoosterGetNumFeature(md->h, &nf) != 0 || nf != md->n_feats) {
        fprintf(stderr, "lb_model: %s 입력 개수가 meta.json과 다름 (%d vs %d)\n", path, nf, md->n_feats);
        return -1;
    }
    return 0;
}

LbModel *lb_model_create(const LbModelConfig *cfg) {
    LbModel *m = calloc(1, sizeof(*m));
    if (!m)
        return NULL;
    m->cfg = *cfg;
    pthread_mutex_init(&m->pods_mtx, NULL);
    cJSON *root = NULL;
    if (cfg->objective == LB_OBJECTIVE_SIMPLE)   // 규칙만 쓰므로 모델 파일을 읽지 않는다
        goto open_log;

    char path[512];
    snprintf(path, sizeof(path), "%s/meta.json", cfg->model_dir);
    char *text = read_file(path);
    root = text ? cJSON_Parse(text) : NULL;
    free(text);
    const cJSON *models = root ? cJSON_GetObjectItem(root, "models") : NULL;
    const cJSON *cap = root ? cJSON_GetObjectItem(root, "eff_cap") : NULL;
    if (!models || !cJSON_IsNumber(cap)) {
        fprintf(stderr, "lb_model: %s 읽기 실패\n", path);
        goto fail;
    }
    m->eff_cap = cap->valuedouble;
    if (load_model(&m->warn, models, "warn", cfg->model_dir, 1) != 0 ||
        load_model(&m->crit, models, "crit", cfg->model_dir, 1) != 0 ||
        load_model(&m->sev, models, "sev", cfg->model_dir, 0) != 0)
        goto fail;
open_log:
    if (cfg->log_path[0] != '\0') {
        m->log = fopen(cfg->log_path, "a");
        if (!m->log) {
            fprintf(stderr, "lb_model: 로그 열기 실패 %s: %s\n", cfg->log_path, strerror(errno));
            goto fail;
        }
    }
    cJSON_Delete(root);
    return m;

fail:
    cJSON_Delete(root);
    lb_model_destroy(m);
    return NULL;
}

// ---- 샘플 → 1초 기록 ----

LbPod *lb_model_add_pod(LbModel *m, const char *key) {
    pthread_mutex_lock(&m->pods_mtx);
    LbPod *p = NULL;
    for (int i = 0; i < m->n_pods && !p; i++)          // 1) 같은 key의 빈 칸 (같은 IP로 다시 뜬 pod)
        if (!m->pods[i].active && !strncmp(m->pods[i].key, key, N2SL_KEY_LEN))
            p = &m->pods[i];
    if (!p && m->n_pods < N2SL_MAX_SRV) {             // 2) 새 칸
        p = &m->pods[m->n_pods++];
        pthread_mutex_init(&p->mtx, NULL);
    }
    for (int i = 0; i < m->n_pods && !p; i++)          // 3) 칸이 다 찼으면 아무 빈 칸
        if (!m->pods[i].active)
            p = &m->pods[i];
    if (!p) {
        pthread_mutex_unlock(&m->pods_mtx);
        fprintf(stderr, "lb_model: pod 추가 불가, 칸 %d개가 모두 사용 중 (%s)\n", N2SL_MAX_SRV, key);
        return NULL;
    }

    pthread_mutex_lock(&p->mtx);                       // 예전 pod의 기록을 지운다
    copy_str(p->key, sizeof(p->key), key);
    p->has_first = 0;
    p->first_sec = p->last_sec = 0;
    p->cpu_limit = 0;
    for (int i = 0; i < RING_SEC; i++)
        p->bin[i].sec = -1;
    pthread_mutex_unlock(&p->mtx);
    p->active = 1;
    p->fresh = 1;
    if (m->assign && m->assign->n_srv < (uint32_t)m->n_pods)
        m->assign->n_srv = (uint32_t)m->n_pods;
    pthread_mutex_unlock(&m->pods_mtx);
    return p;
}

void lb_model_remove_pod(LbModel *m, LbPod *p) {
    if (!m || !p)
        return;
    pthread_mutex_lock(&m->pods_mtx);
    p->active = 0;                                     // key는 남긴다 (헤더 설명 참고)
    pthread_mutex_unlock(&m->pods_mtx);
}

void lb_model_add_sample(LbPod *p, int64_t recv_ms, double cpu_util_pct, double thr_usec,
                         uint32_t viewer_count, double cpu_limit_cores) {
    int64_t s = recv_ms / 1000;
    pthread_mutex_lock(&p->mtx);
    if (!p->has_first) {
        p->has_first = 1;
        p->first_sec = s;
    }
    p->last_sec = s;
    SecBin *b = &p->bin[s & (RING_SEC - 1)];
    if (b->sec != s) {
        memset(b, 0, sizeof(*b));
        b->sec = s;
        b->cpu_max = -INFINITY;
    }
    if (!isnan(cpu_util_pct)) {
        b->cpu_sum += cpu_util_pct;
        b->cpu_n++;
        if (cpu_util_pct > b->cpu_max)
            b->cpu_max = cpu_util_pct;
    }
    if (!isnan(thr_usec))
        b->thr_us += thr_usec;
    b->vc = viewer_count;
    b->has_vc = 1;
    if (cpu_limit_cores > 0)
        p->cpu_limit = cpu_limit_cores;
    pthread_mutex_unlock(&p->mtx);
}

// ---- 1초 기록 → 입력 (20_headroom_mexp.cpu_table과 같은 계산) ----

static const SecBin *get_bin(const LbPod *p, int64_t s) {
    const SecBin *b = &p->bin[s & (RING_SEC - 1)];
    return b->sec == s ? b : NULL;
}

static int viewer_at(const LbPod *p, int64_t t, double *out) {
    for (int64_t k = t; k > t - V_LOOKBACK; k--) {
        const SecBin *b = get_bin(p, k);
        if (b && b->has_vc) {
            *out = b->vc;
            return 1;
        }
    }
    return 0;
}

static double thr_sum(const LbPod *p, int64_t t, int w) {
    double us = 0;
    for (int64_t k = t - w + 1; k <= t; k++) {
        const SecBin *b = get_bin(p, k);
        if (b)
            us += b->thr_us;
    }
    return us / 1e6;
}

static int pod_features(LbPod *p, int64_t t, LbFeatures *f) {
    int ok = 0;
    pthread_mutex_lock(&p->mtx);
    double V, V10;
    if (p->has_first && t - 10 >= p->first_sec && t - p->last_sec <= STALE_SEC && p->cpu_limit > 0 &&
        viewer_at(p, t, &V) && viewer_at(p, t - 10, &V10)) {
        double mean_sum = 0, peak = NAN;
        int mean_n = 0;
        for (int64_t k = t - 9; k <= t; k++) {
            const SecBin *b = get_bin(p, k);
            if (b && b->cpu_n > 0) {
                mean_sum += b->cpu_sum / b->cpu_n;
                mean_n++;
                if (k >= t - 4 && (isnan(peak) || b->cpu_max > peak))
                    peak = b->cpu_max;
            }
        }
        if (mean_n > 0) {
            double Vs = V > 1 ? V : 1;
            f->V = V;
            f->u = mean_sum / mean_n / Vs;
            f->u_peak = peak / Vs;
            f->dV10 = V - V10;
            f->past10 = thr_sum(p, t, 10);
            f->past30 = thr_sum(p, t, 30);
            f->cpu_limit = p->cpu_limit;
            ok = 1;
        }
    }
    pthread_mutex_unlock(&p->mtx);
    return ok;
}

// ---- 표 계산 (lb_core.build_table과 같은 계산) ----

static double feat_value(int id, const LbFeatures *f, double N, double eff_cap) {
    double util = f->V * f->u;
    switch (id) {
    case F_UTIL:    return util;
    case F_PEAK:    return f->V * f->u_peak;
    case F_INC:     return N * f->u;
    case F_PROJ:    return util + N * f->u;
    case F_DUTIL10: return f->dV10 * f->u;
    case F_PAST10:  return f->past10;
    case F_PAST30:  return f->past30;
    case F_EFF:     return f->cpu_limit < eff_cap ? f->cpu_limit : eff_cap;
    }
    return NAN;
}

static int predict(const LbModel *m, const Model *md, double *xrow, const LbFeatures *fs, const double *pend,
                   const int *rows_of, int n_pods, int k_rows, double *out) {
    size_t r = 0;
    for (int i = 0; i < n_pods; i++)
        for (int k = 0; k < k_rows; k++, r++)
            for (int c = 0; c < md->n_feats; c++)
                xrow[r * (size_t)md->n_feats + (size_t)c] =
                    feat_value(md->feats[c], &fs[rows_of[i]], k + pend[rows_of[i]], m->eff_cap);
    char param[32];
    snprintf(param, sizeof(param), "num_threads=%d", m->cfg.num_threads);
    int64_t got = 0;
    if (LGBM_BoosterPredictForMat(md->h, xrow, C_API_DTYPE_FLOAT64, (int32_t)r, md->n_feats, 1, C_API_PREDICT_NORMAL, 0, -1,
                                  param, &got, out) != 0 || got != (int64_t)r) {
        fprintf(stderr, "lb_model: 예측 실패: %s\n", LGBM_GetLastError());
        return -1;
    }
    return 0;
}

static double calibrate(const Model *md, double raw) {
    if (raw < 1e-4)
        raw = 1e-4;
    if (raw > 1 - 1e-4)
        raw = 1 - 1e-4;
    return 1.0 / (1.0 + exp(-(md->platt_a * log(raw / (1 - raw)) + md->platt_b)));
}

static int pod_simple(LbPod *p, int64_t t, LbFeatures *f);
static void simple_table(LbModel *m);

static void read_assigned(const LbModel *m, uint32_t *out) {
    for (int i = 0; i < m->n_pods; i++)
        out[i] = m->assign ? atomic_load_explicit((const _Atomic uint32_t *)&m->assign->assigned[i], memory_order_relaxed) : 0;
}

static int compute(LbModel *m, int64_t t, const uint32_t *assigned) {
    LbResult *R = &m->res;
    int n = m->n_pods, rows_of[N2SL_MAX_SRV], n_ok = 0, simple = m->cfg.objective == LB_OBJECTIVE_SIMPLE;
    R->n = n;
    R->t = t;

    // 막 (다시) 쓰기 시작한 칸: 이전에 이 칸 번호로 배정된 인원을 새 pod의 pending으로 세지 않게 기록을 지금 값으로 맞춘다
    for (int i = 0; i < n; i++)
        if (m->pods[i].fresh) {
            for (int h = 0; h < HIST_SEC; h++)
                m->hist[h][i] = assigned[i];
            m->pods[i].fresh = 0;
        }

    memcpy(m->hist[m->hist_pos], assigned, sizeof(uint32_t) * (size_t)n);
    int oldest = m->hist_n < HIST_SEC ? 0 : (m->hist_pos + 1) % HIST_SEC;
    int have_hist = m->hist_n >= 1;
    for (int i = 0; i < n; i++) {
        if (!m->pods[i].active)                  // 사라진 pod의 칸: 입력 없음 → 아래에서 자리 0, 비용 최대
            R->ok[i] = 0;
        else
            R->ok[i] = simple ? pod_simple(&m->pods[i], t, &R->f[i]) : pod_features(&m->pods[i], t, &R->f[i]);
        double sent = have_hist ? (double)(uint32_t)(assigned[i] - m->hist[oldest][i]) : 0;
        double seen = R->ok[i] && R->f[i].dV10 > 0 ? R->f[i].dV10 : 0;
        R->pending[i] = sent > seen ? sent - seen : 0;
        if (R->ok[i])
            rows_of[n_ok++] = i;
    }
    m->hist_pos = (m->hist_pos + 1) % HIST_SEC;
    if (m->hist_n < HIST_SEC)
        m->hist_n++;

    if (simple) {
        for (int i = 0; i < n; i++)
            if (!R->ok[i]) {                     // 입력이 없는 pod: 자리 0, 비용이 항상 가장 크게
                R->seats[i] = 0;
                R->p_warn0[i] = R->p_crit0[i] = 0;
                for (int k = 0; k < K_ROWS; k++)
                    R->cost[i][k] = 1e6f * (float)(k + 1);
            }
        simple_table(m);
        return 0;
    }
    if (n_ok > 0) {
        if (predict(m, &m->warn, m->xrow, R->f, R->pending, rows_of, n_ok, K_ROWS, m->out_warn) != 0 ||
            predict(m, &m->sev, m->xrow, R->f, R->pending, rows_of, n_ok, K_ROWS, m->out_sev) != 0 ||
            predict(m, &m->crit, m->xrow, R->f, R->pending, rows_of, n_ok, 1, m->out_crit) != 0)
            return -1;
    }

    for (int i = 0; i < n; i++) {           // 입력이 없는 pod: 자리 0, 비용 증가가 항상 가장 크게
        if (R->ok[i])
            continue;
        R->seats[i] = 0;
        R->p_warn0[i] = R->p_crit0[i] = 0;
        for (int k = 0; k < K_ROWS; k++)
            R->cost[i][k] = 1e6f * (float)(k + 1);
    }
    for (int j = 0; j < n_ok; j++) {
        int i = rows_of[j];
        const LbFeatures *f = &R->f[i];
        int seats = N2SL_KMAX, found = 0;
        for (int k = 0; k < K_ROWS; k++) {
            double pw = calibrate(&m->warn, m->out_warn[(size_t)j * K_ROWS + (size_t)k]);
            double sev = expm1(m->out_sev[(size_t)j * K_ROWS + (size_t)k]);
            double c = pw * (sev > 0 ? sev : 0);
            if (m->cfg.objective == LB_OBJECTIVE_PEOPLE)
                c *= f->V + R->pending[i] + k;
            R->cost[i][k] = (float)c;
            if (k == 0)
                R->p_warn0[i] = (float)pw;
            if (!found && k <= N2SL_KMAX && pw > m->cfg.p_th) {
                seats = k > 0 ? k - 1 : 0;
                found = 1;
            }
        }
        double lim = f->cpu_limit, per_pct = f->u / 100 * lim;     // 용량 규칙
        double used = f->V * per_pct + R->pending[i] * per_pct;
        double per = per_pct > PER_VIEWER_CORE ? per_pct : PER_VIEWER_CORE;
        double cap = floor(((lim < m->eff_cap ? lim : m->eff_cap) - used) / per);
        if (cap < 0)
            cap = 0;
        R->seats[i] = cap < seats ? (int32_t)cap : seats;
        R->p_crit0[i] = (float)calibrate(&m->crit, m->out_crit[j]);
    }
    return 0;
}

// ---- simple: 모델 없이 규칙으로 표 만들기 ----
// 1) 자리가 남은 서버(예상 이용률 < sat_pct)가 있으면 그중 예상 이용률이 가장 낮은 곳
// 2) 없으면 throttle이 없는 서버 중 예상 이용률이 가장 낮은 곳
// 3) 그것도 없으면 예상 throttle = thr/V × (V + 이번에 보낸 인원)이 가장 낮은 곳
// 예상 이용률 = util + d × (pending + k), d = util / V (서버별 시청자 1명당 이용률)
// n2sl_pick은 cost[보낸 인원 + 1]을 그대로 비교하므로 단계마다 비용 범위가 겹치지 않게 둔다

static int pod_simple(LbPod *p, int64_t t, LbFeatures *f) {
    int ok = 0;
    pthread_mutex_lock(&p->mtx);
    double V, V10;
    if (p->has_first && t - p->last_sec <= STALE_SEC && viewer_at(p, t, &V)) {
        const SecBin *b = get_bin(p, t);
        if (!b || b->cpu_n == 0)                 // 마지막 초 샘플이 아직 없으면 한 초 전 값
            b = get_bin(p, t - 1);
        if (b && b->cpu_n > 0) {
            memset(f, 0, sizeof(*f));
            f->V = V;
            f->util1 = b->cpu_sum / b->cpu_n;
            f->u = f->u_peak = NAN;
            f->dV10 = viewer_at(p, t - 10, &V10) ? V - V10 : 0;
            f->thr1 = thr_sum(p, t, 1);
            f->thr3 = thr_sum(p, t, 3) / 3;
            f->past10 = thr_sum(p, t, 10);
            f->past30 = thr_sum(p, t, 30);
            f->cpu_limit = p->cpu_limit;
            ok = 1;
        }
    }
    pthread_mutex_unlock(&p->mtx);
    return ok;
}

static void simple_table(LbModel *m) {
    LbResult *R = &m->res;
    double d_sum = 0;
    int d_n = 0;
    for (int i = 0; i < R->n; i++) {             // 1명당 이용률. 시청자가 없으면 아래에서 다른 서버 평균으로
        LbFeatures *f = &R->f[i];
        f->d = R->ok[i] && f->V > 0 && f->util1 > 0 ? f->util1 / f->V : NAN;
        if (!isnan(f->d)) {
            d_sum += f->d;
            d_n++;
        }
    }
    double d_fill = d_n > 0 ? d_sum / d_n : m->cfg.d_default;
    double sat = m->cfg.sat_pct;
    for (int i = 0; i < R->n; i++) {
        if (!R->ok[i])
            continue;
        LbFeatures *f = &R->f[i];
        if (isnan(f->d))
            f->d = d_fill;
        double pend = R->pending[i], d = f->d;
        int seats = 0;
        if (f->util1 < sat && f->thr1 <= 0) {    // 포화 = 이용률이 sat_pct 이상이거나 마지막 1초에 throttle
            double s = floor((sat - f->util1) / d - pend);
            seats = s < 0 ? 0 : s > N2SL_KMAX ? N2SL_KMAX : (int)s;
        }
        double thr_pv = f->thr3 * 100 / (f->V > 1 ? f->V : 1);  // 1명당 throttle (%)
        for (int k = 0; k < K_ROWS; k++) {
            double proj = f->util1 + d * (pend + k), c;
            if (k <= seats)
                c = proj;
            else if (f->thr3 <= 0)
                c = SIMPLE_TIER2 + proj;
            else
                c = SIMPLE_TIER3 + thr_pv * (f->V + pend + k);
            R->cost[i][k] = (float)c;
        }
        R->seats[i] = seats;
        R->p_warn0[i] = R->p_crit0[i] = 0;
    }
}

const LbResult *lb_model_compute(LbModel *m, int64_t t) {
    uint32_t assigned[N2SL_MAX_SRV] = {0};
    pthread_mutex_lock(&m->pods_mtx);
    read_assigned(m, assigned);
    int rc = compute(m, t, assigned);
    pthread_mutex_unlock(&m->pods_mtx);
    return rc == 0 ? &m->res : NULL;
}

// ---- 공유메모리 ----

static void *map_shm(const char *name, size_t size) {
    int fd = shm_open(name, O_CREAT | O_RDWR, 0666);
    if (fd < 0) {
        fprintf(stderr, "lb_model: shm_open %s: %s\n", name, strerror(errno));
        return NULL;
    }
    fchmod(fd, 0666);   /* umask와 상관없이 HAProxy(n2sl 사용자)가 assign에 쓸 수 있게 */
    void *p = MAP_FAILED;
    if (ftruncate(fd, (off_t)size) == 0)
        p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED)
        fprintf(stderr, "lb_model: %s 매핑 실패: %s\n", name, strerror(errno));
    close(fd);
    return p == MAP_FAILED ? NULL : p;
}

static int64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void write_table(LbModel *m, const uint32_t *base) {
    n2sl_table_t *t = m->table;
    const LbResult *R = &m->res;
    uint32_t s = atomic_load_explicit((_Atomic uint32_t *)&t->seq, memory_order_relaxed);
    atomic_store_explicit((_Atomic uint32_t *)&t->seq, s + 1, memory_order_relaxed);
    atomic_thread_fence(memory_order_release);

    t->magic = N2SL_MAGIC;
    t->version = N2SL_VERSION;
    t->n_srv = (uint32_t)R->n;
    for (int i = 0; i < R->n; i++) {
        n2sl_srv_t *d = &t->srv[i];
        memcpy(d->key, m->pods[i].key, N2SL_KEY_LEN);
        d->seats = R->seats[i];
        d->base_assigned = base[i];
        d->p_warn0 = R->p_warn0[i];
        d->p_crit0 = R->p_crit0[i];
        memcpy(d->cost, R->cost[i], sizeof(d->cost));
    }
    t->epoch++;
    t->built_ms = now_ms();
    atomic_store_explicit((_Atomic uint32_t *)&t->seq, s + 2, memory_order_release);
}

static void write_log(LbModel *m, double ms) {
    const LbResult *R = &m->res;
    fprintf(m->log, "{\"t\":%lld,\"ms\":%.1f,\"pods\":[", (long long)R->t, ms);
    for (int i = 0; i < R->n; i++)
        if (m->cfg.objective == LB_OBJECTIVE_SIMPLE)
            fprintf(m->log, "%s{\"key\":\"%s\",\"ok\":%d,\"V\":%.0f,\"seats\":%d,\"pending\":%.0f,\"util\":%.2f,\"d\":%.3f,\"thr3\":%.4f,\"c1\":%.3f}",
                    i ? "," : "", m->pods[i].key, R->ok[i], R->ok[i] ? R->f[i].V : -1.0, R->seats[i], R->pending[i],
                    R->ok[i] ? R->f[i].util1 : -1.0, R->ok[i] ? R->f[i].d : -1.0, R->ok[i] ? R->f[i].thr3 : -1.0,
                    (double)R->cost[i][1]);
        else
            fprintf(m->log, "%s{\"key\":\"%s\",\"ok\":%d,\"V\":%.0f,\"seats\":%d,\"pending\":%.0f,\"p_warn0\":%.4f,\"p_crit0\":%.4f}",
                    i ? "," : "", m->pods[i].key, R->ok[i], R->ok[i] ? R->f[i].V : -1.0, R->seats[i], R->pending[i],
                    R->p_warn0[i], R->p_crit0[i]);
    fprintf(m->log, "]}\n");
    fflush(m->log);
}

// ---- 1초 주기 스레드 ----

static void *model_main(void *arg) {
    LbModel *m = arg;
    struct timespec wake;
    clock_gettime(CLOCK_REALTIME, &wake);
    wake.tv_sec += 1;
    wake.tv_nsec = WAKE_AFTER_NS;
    while (atomic_load(&m->running)) {
        while (clock_nanosleep(CLOCK_REALTIME, TIMER_ABSTIME, &wake, NULL) == EINTR)
            ;
        if (!atomic_load(&m->running))
            break;
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        pthread_mutex_lock(&m->pods_mtx);         // 계산·표 쓰기 중에 main의 sync가 칸을 바꾸지 않게
        uint32_t assigned[N2SL_MAX_SRV];
        read_assigned(m, assigned);               // 표에 적을 base. 이후 배정은 used로 잡힌다
        if (compute(m, (int64_t)wake.tv_sec - 1, assigned) == 0) {
            int any_ok = 0;
            for (int i = 0; i < m->res.n; i++)
                any_ok |= m->res.ok[i];
            if (any_ok)                           // 입력이 있는 pod가 없으면 쓰지 않는다 → 표가 오래되어 HAProxy가 기본 알고리즘으로
                write_table(m, assigned);
        }                                         // 예측이 실패해도 쓰지 않는다
        clock_gettime(CLOCK_MONOTONIC, &t1);
        double ms = (double)(t1.tv_sec - t0.tv_sec) * 1000 + (double)(t1.tv_nsec - t0.tv_nsec) / 1e6;
        if (m->log)
            write_log(m, ms);
        pthread_mutex_unlock(&m->pods_mtx);

        wake.tv_sec += 1;
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        if (now.tv_sec > wake.tv_sec)             // 밀렸으면 건너뛴다
            wake.tv_sec = now.tv_sec + 1;
    }
    return NULL;
}

int lb_model_start(LbModel *m) {
    if (m->n_pods == 0)   // pod는 k8s 목록 갱신으로 나중에 들어올 수 있다. 그 전엔 표를 쓰지 않음 → HAProxy는 round robin
        fprintf(stderr, "lb_model: 아직 pod가 없음 (목록 갱신을 기다림)\n");
    m->table = map_shm(m->cfg.table_shm, sizeof(n2sl_table_t));
    m->assign = map_shm(m->cfg.assign_shm, sizeof(n2sl_assign_t));
    if (!m->table || !m->assign)
        return -1;
    uint32_t s = atomic_load((_Atomic uint32_t *)&m->table->seq);
    if (s & 1u)                                    // 쓰는 중에 죽은 이전 프로세스의 흔적
        atomic_store((_Atomic uint32_t *)&m->table->seq, s + 1);
    pthread_mutex_lock(&m->pods_mtx);
    m->assign->n_srv = (uint32_t)m->n_pods;        // 누적 배정 수는 지우지 않는다 (HAProxy가 쓰는 값)
    pthread_mutex_unlock(&m->pods_mtx);
    m->assign->version = N2SL_VERSION;
    m->assign->magic = N2SL_MAGIC;
    atomic_store(&m->running, 1);
    if (pthread_create(&m->thread, NULL, model_main, m) != 0) {
        fprintf(stderr, "lb_model: 스레드 시작 실패\n");
        return -1;
    }
    m->thread_started = 1;
    return 0;
}

void lb_model_stop(LbModel *m) {
    if (!m->thread_started)
        return;
    atomic_store(&m->running, 0);
    pthread_join(m->thread, NULL);
    m->thread_started = 0;
}

void lb_model_destroy(LbModel *m) {
    if (!m)
        return;
    lb_model_stop(m);
    if (m->warn.h)
        LGBM_BoosterFree(m->warn.h);
    if (m->crit.h)
        LGBM_BoosterFree(m->crit.h);
    if (m->sev.h)
        LGBM_BoosterFree(m->sev.h);
    if (m->table)
        munmap(m->table, sizeof(n2sl_table_t));
    if (m->assign)
        munmap(m->assign, sizeof(n2sl_assign_t));
    if (m->log)
        fclose(m->log);
    for (int i = 0; i < m->n_pods; i++)
        pthread_mutex_destroy(&m->pods[i].mtx);
    pthread_mutex_destroy(&m->pods_mtx);
    free(m);
}
