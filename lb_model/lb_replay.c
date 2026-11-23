// lb_model 점검 도구 (서버, HAProxy 없이)
//   table: 기록된 CSV를 실제 시각 순서로 lb_model_add_sample에 넣고, 초마다 입력값과 표를 CSV로 출력
//     lb_replay table <lb_model.conf> <out.csv> <pod1.csv> [pod2.csv ...]
//   shm:   모델 스레드가 표를 쓰는 동안 n2sl_pick으로 계속 읽어 깨진 표가 없는지 확인
//     lb_replay shm <lb_model.conf> <초> <pod1.csv> [pod2.csv ...]
#define _GNU_SOURCE
#include "lb_model.h"

#include <fcntl.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    FILE *f;
    int i_ms, i_cpu, i_thr, i_vc, i_lim;
    int64_t ms;
    double cpu, thr, vc, lim;
    int done;
} Reader;

static int col_index(char *header, const char *name) {
    char *copy = strdup(header), *save = NULL;
    int i = 0, found = -1;
    for (char *t = strtok_r(copy, ",\r\n", &save); t; t = strtok_r(NULL, ",\r\n", &save), i++)
        if (!strcmp(t, name))
            found = i;
    free(copy);
    return found;
}

// strtok는 빈 칸을 건너뛰므로 직접 나눈다. 빈 칸은 NAN
static int next_row(Reader *r) {
    char line[4096];
    if (!fgets(line, sizeof(line), r->f)) {
        r->done = 1;
        return 0;
    }
    r->ms = -1;
    r->cpu = r->thr = r->vc = r->lim = NAN;
    int i = 0;
    char *p = line;
    while (p) {
        char *comma = strchr(p, ',');
        if (comma)
            *comma = '\0';
        double v = (*p && *p != '\r' && *p != '\n') ? atof(p) : NAN;
        if (i == r->i_ms)
            r->ms = (int64_t)v;
        else if (i == r->i_cpu)
            r->cpu = v;
        else if (i == r->i_thr)
            r->thr = v;
        else if (i == r->i_vc)
            r->vc = v;
        else if (i == r->i_lim)
            r->lim = v;
        p = comma ? comma + 1 : NULL;
        i++;
    }
    return 1;
}

static int open_reader(Reader *r, const char *path) {
    memset(r, 0, sizeof(*r));
    r->f = fopen(path, "r");
    char line[4096];
    if (!r->f || !fgets(line, sizeof(line), r->f)) {
        fprintf(stderr, "CSV 열기 실패: %s\n", path);
        return -1;
    }
    r->i_ms = col_index(line, "recv_ms");
    r->i_cpu = col_index(line, "cpu_util_pct");
    r->i_thr = col_index(line, "cpu_thr_usec");
    r->i_vc = col_index(line, "viewer_count");
    r->i_lim = col_index(line, "cpu_limit_cores");
    if (r->i_ms < 0 || r->i_cpu < 0 || r->i_thr < 0 || r->i_vc < 0 || r->i_lim < 0) {
        fprintf(stderr, "CSV 열 없음: %s\n", path);
        return -1;
    }
    next_row(r);
    return 0;
}

static void feed(LbPod *p, const Reader *r, int64_t shift_ms) {
    lb_model_add_sample(p, r->ms + shift_ms, r->cpu, r->thr, isnan(r->vc) ? 0 : (uint32_t)r->vc, r->lim);
}

static int run_table(LbModel *m, LbPod **pods, Reader *rd, int n, const char *out_path) {
    FILE *out = fopen(out_path, "w");
    if (!out)
        return -1;
    fprintf(out, "t,pod,ok,V,u,u_peak,dV10,past10,past30,cpu_limit,seats,p_warn0,p_crit0,c0,c5,c20,c100,c101\n");
    int64_t cur = -1;
    for (;;) {
        int b = -1;
        for (int i = 0; i < n; i++)
            if (!rd[i].done && (b < 0 || rd[i].ms < rd[b].ms))
                b = i;
        int64_t sec = b >= 0 ? rd[b].ms / 1000 : cur + 1;
        if (cur >= 0 && sec > cur) {
            const LbResult *R = lb_model_compute(m, cur);
            if (!R)
                return -1;
            for (int i = 0; i < n; i++) {
                const LbFeatures *f = &R->f[i];
                const float *c = R->cost[i];
                if (R->ok[i])
                    fprintf(out, "%lld,%d,1,%.17g,%.17g,%.17g,%.17g,%.17g,%.17g,%.17g,%d,%.8g,%.8g,%.8g,%.8g,%.8g,%.8g,%.8g\n",
                            (long long)cur, i, f->V, f->u, f->u_peak, f->dV10, f->past10, f->past30, f->cpu_limit, R->seats[i],
                            R->p_warn0[i], R->p_crit0[i], c[0], c[5], c[20], c[100], c[101]);
                else
                    fprintf(out, "%lld,%d,0,,,,,,,,0,,,,,,,\n", (long long)cur, i);
            }
        }
        if (b < 0)
            break;
        cur = sec;
        feed(pods[b], &rd[b], 0);
        next_row(&rd[b]);
    }
    fclose(out);
    return 0;
}

static int64_t wall_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

// CSV 시각을 지금으로 옮겨 실시간처럼 넣으면서, 다른 쪽에서 n2sl_pick을 계속 부른다
static int run_shm(LbModel *m, LbPod **pods, Reader *rd, int n, const LbModelConfig *cfg, int seconds) {
    int64_t start = wall_ms(), origin = rd[0].ms;
    for (int i = 1; i < n; i++)
        if (rd[i].ms < origin)
            origin = rd[i].ms;
    int64_t skip_ms = 300 * 1000;                     // 앞 300초(기록의 입력 준비 구간 포함)는 한꺼번에 넣는다
    int64_t shift = start - skip_ms - origin;
    for (;;) {
        int fed = 0;
        for (int i = 0; i < n; i++)
            while (!rd[i].done && rd[i].ms + shift <= start) {
                feed(pods[i], &rd[i], shift);
                next_row(&rd[i]);
                fed = 1;
            }
        if (!fed)
            break;
    }
    if (lb_model_start(m) != 0)
        return -1;

    int tfd = shm_open(cfg->table_shm, O_RDONLY, 0), afd = shm_open(cfg->assign_shm, O_RDWR, 0);
    const n2sl_table_t *t = mmap(NULL, sizeof(*t), PROT_READ, MAP_SHARED, tfd, 0);
    n2sl_assign_t *a = mmap(NULL, sizeof(*a), PROT_READ | PROT_WRITE, MAP_SHARED, afd, 0);
    if (t == MAP_FAILED || a == MAP_FAILED)
        return -1;
    unsigned char alive[N2SL_MAX_SRV];
    memset(alive, 1, sizeof(alive));

    long calls = 0, fails = 0, torn = 0, counts[N2SL_MAX_SRV] = {0};
    uint64_t last_epoch = 0;
    int64_t end = start + seconds * 1000L;
    struct timespec pace = {0, 20000000};             // 초당 약 50명
    while (wall_ms() < end) {
        for (int i = 0; i < n; i++)
            while (!rd[i].done && rd[i].ms + shift <= wall_ms()) {
                feed(pods[i], &rd[i], shift);
                next_row(&rd[i]);
            }
        int k = n2sl_pick(t, a, alive, wall_ms(), 3000);
        calls++;
        if (k < 0)
            fails++;
        else
            counts[k]++;
        // 같은 번호 안에서 표가 일관적인지 확인 (seats가 범위 안, cost가 유한)
        uint32_t s1 = __atomic_load_n(&t->seq, __ATOMIC_ACQUIRE);
        if (!(s1 & 1u)) {
            int bad = 0;
            for (uint32_t i = 0; i < t->n_srv; i++)
                if (t->srv[i].seats < 0 || t->srv[i].seats > N2SL_KMAX || !isfinite(t->srv[i].cost[0]))
                    bad = 1;
            __atomic_thread_fence(__ATOMIC_ACQUIRE);
            if (bad && __atomic_load_n(&t->seq, __ATOMIC_RELAXED) == s1)
                torn++;
        }
        if (t->epoch != last_epoch) {
            last_epoch = t->epoch;
            printf("epoch %llu:", (unsigned long long)last_epoch);
            for (uint32_t i = 0; i < t->n_srv; i++)
                printf(" %s seats=%d used=%u p_warn=%.3f", t->srv[i].key, t->srv[i].seats,
                       a->assigned[i] - t->srv[i].base_assigned, t->srv[i].p_warn0);
            printf("\n");
        }
        nanosleep(&pace, NULL);
    }
    lb_model_stop(m);
    printf("pick %ld번, 실패(-1) %ld번, 깨진 표 %ld번, 배정:", calls, fails, torn);
    for (int i = 0; i < n; i++)
        printf(" %ld", counts[i]);
    printf("\n");
    int fresh = n2sl_pick(t, a, alive, wall_ms(), 3000);
    sleep(4);                                         // 표가 오래되면 -1이어야 한다 (HAProxy는 기본 알고리즘으로)
    int stale = n2sl_pick(t, a, alive, wall_ms(), 3000);
    printf("스레드 종료 직후 pick %d, 4초 뒤 pick %d\n", fresh, stale);
    return torn == 0 && stale == -1 ? 0 : 1;
}

int main(int argc, char **argv) {
    if (argc < 5) {
        fprintf(stderr, "usage: lb_replay table <conf> <out.csv> <pod.csv>... | lb_replay shm <conf> <초> <pod.csv>...\n");
        return 2;
    }
    LbModelConfig cfg;
    if (lb_model_config_load(argv[2], &cfg) != 0)
        return 1;
    LbModel *m = lb_model_create(&cfg);
    if (!m)
        return 1;
    int n = argc - 4;
    if (n > N2SL_MAX_SRV)
        return 2;
    LbPod *pods[N2SL_MAX_SRV];
    Reader *rd = calloc((size_t)n, sizeof(Reader));
    for (int i = 0; i < n; i++) {
        char key[32];
        snprintf(key, sizeof(key), "edge%d", i);
        pods[i] = lb_model_add_pod(m, key);
        if (!pods[i] || open_reader(&rd[i], argv[4 + i]) != 0)
            return 1;
    }
    int rc = !strcmp(argv[1], "table") ? run_table(m, pods, rd, n, argv[3])
                                       : run_shm(m, pods, rd, n, &cfg, atoi(argv[3]));
    lb_model_destroy(m);
    return rc == 0 ? 0 : 1;
}
