#include "metric_poster.h"

#include <cjson/cJSON.h>
#include <curl/curl.h>
#include <errno.h>
#include <math.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define DEFAULT_TIMEOUT_MS 500
#define CONNECT_TIMEOUT_MS 300
#define RESP_MAX 256                 // 실패 로그에 붙일 응답 본문 앞부분

// 이름별 직전 전송 시점의 원본값 (Δ 기준)
typedef struct {
    char name[METRIC_POSTER_NAME_LEN];
    SystemInfo info;
} Prev;

struct MetricPoster {
    MetricPosterConfig cfg;
    MetricPosterReadFn read;
    void *ctx;

    // read 결과 (매초 덮어씀)
    MetricPosterNode nodes[METRIC_POSTER_MAX];
    MetricPosterPod pods[METRIC_POSTER_MAX];
    size_t node_count, pod_count;

    // 기준값. 매초 지금 값으로 통째로 바꾼다 → 사라진 노드·pod는 자연히 빠진다
    Prev prev_nodes[METRIC_POSTER_MAX], next_nodes[METRIC_POSTER_MAX];
    Prev prev_pods[METRIC_POSTER_MAX], next_pods[METRIC_POSTER_MAX];
    size_t prev_node_count, prev_pod_count;

    CURL *curl;
    struct curl_slist *headers;
    char resp[RESP_MAX];
    size_t resp_len;

    int state;                       // -1 아직 안 보냄, 0 실패 중, 1 성공 중 (바뀔 때만 로그)
    int64_t fail_since_ms;

    pthread_t thread;
    atomic_int running;
    int thread_started;
};

static int64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static const Prev *find_prev(const Prev *list, size_t n, const char *name) {
    for (size_t i = 0; i < n; i++)
        if (strcmp(list[i].name, name) == 0)
            return &list[i];
    return NULL;
}

// NAN이면 null. 소수 둘째 자리까지
static void add_num(cJSON *o, const char *key, double v) {
    if (isnan(v) || isinf(v))
        cJSON_AddNullToObject(o, key);
    else
        cJSON_AddNumberToObject(o, key, round(v * 100.0) / 100.0);
}

static double age_ms(const SystemInfo *info, int64_t now) {
    double a = (double)(now - info->recv_ms);
    return a < 0 ? 0 : a;
}

// ---- Δ 계산 (cur − prev). 계산할 수 없으면 NAN ----

static void node_values(const SystemInfo *cur, const SystemInfo *prev, double *cpu_pct, double *drops_per_sec) {
    *cpu_pct = NAN;
    *drops_per_sec = NAN;
    if (!prev || prev->kind != SYSINFO_KIND_NODE)
        return;                                    // 첫 전송
    const NodeInfo *c = &cur->node, *p = &prev->node;
    double dt_ms = (double)(cur->recv_ms - prev->recv_ms);

    if (c->has_cpu && p->has_cpu) {
        int64_t dtotal = c->cpu_total - p->cpu_total;
        int64_t didle = c->cpu_idle - p->cpu_idle;
        if (dtotal > 0 && didle >= 0 && didle <= dtotal)   // dtotal==0: 1초 동안 새 샘플 없음, 음수: 리셋
            *cpu_pct = (1.0 - (double)didle / (double)dtotal) * 100.0;
    }
    if (c->has_qdisc && p->has_qdisc && dt_ms > 0)          // 32비트 넘침은 uint32 뺄셈 (calc_node_rates와 같음)
        *drops_per_sec = (double)(uint32_t)(c->qdisc_drops - p->qdisc_drops) * 1000.0 / dt_ms;
}

static void pod_values(const SystemInfo *cur, const SystemInfo *prev,
                       double *cpu_pct, double *thr_ms, double *thr_pct) {
    *cpu_pct = NAN;
    *thr_ms = NAN;
    *thr_pct = NAN;
    if (!prev || prev->kind != SYSINFO_KIND_CONTAINER)
        return;
    const ContainerInfo *c = &cur->container, *p = &prev->container;
    double dt_ms = (double)(cur->recv_ms - prev->recv_ms);

    if (!c->has_cpu || !p->has_cpu || dt_ms <= 0)
        return;
    if (c->cpu_usage_usec < p->cpu_usage_usec || c->cpu_nr_periods < p->cpu_nr_periods ||
        c->cpu_nr_throttled < p->cpu_nr_throttled || c->cpu_throttled_usec < p->cpu_throttled_usec)
        return;                                    // 리셋 (컨테이너 재시작, cgroup 재탐색)

    if (c->cpu_limit_cores > 0)                    // 무제한(-1)이면 한도 대비 사용률은 null
        *cpu_pct = (double)(c->cpu_usage_usec - p->cpu_usage_usec) / (dt_ms * 1000.0) / c->cpu_limit_cores * 100.0;
    *thr_ms = (double)(c->cpu_throttled_usec - p->cpu_throttled_usec) / 1000.0;

    uint64_t dperiods = c->cpu_nr_periods - p->cpu_nr_periods;
    if (dperiods == 0) {
        *thr_pct = 0;                              // CPU를 거의 안 써서 주기가 안 돌았음
    } else {
        double v = (double)(c->cpu_nr_throttled - p->cpu_nr_throttled) / (double)dperiods * 100.0;
        *thr_pct = v > 100.0 ? 100.0 : v;
    }
}

// CPU 최대 = 그 1초 안의 0.1초 구간 사용률 중 최대 (app_collector가 쌓고 read가 비움).
// 1초 사용률(cpu)은 0.1초 값들의 평균이라 원래 최대보다 클 수 없지만, 구간 경계가 조금 어긋날 수 있어 작으면 cpu로 올린다.
// 0.1초 값이 없으면(첫 전송, 한도 무제한 등) cpu 하나뿐이면 그 값, 둘 다 없으면 NAN.
static double peak_of(double peak, double cpu) {
    if (isnan(peak)) return cpu;
    if (!isnan(cpu) && cpu > peak) return cpu;
    return peak;
}

// ---- 읽기 + JSON ----

static int read_slots(MetricPoster *p) {
    size_t nn = 0, np = 0;
    if (p->read(p->ctx, p->nodes, METRIC_POSTER_MAX, &nn, p->pods, METRIC_POSTER_MAX, &np) != 0)
        return -1;
    p->node_count = nn > METRIC_POSTER_MAX ? METRIC_POSTER_MAX : nn;
    p->pod_count = np > METRIC_POSTER_MAX ? METRIC_POSTER_MAX : np;
    for (size_t i = 0; i < p->node_count; i++)
        p->nodes[i].name[METRIC_POSTER_NAME_LEN - 1] = '\0';
    for (size_t i = 0; i < p->pod_count; i++) {
        p->pods[i].name[METRIC_POSTER_NAME_LEN - 1] = '\0';
        p->pods[i].node[METRIC_POSTER_NAME_LEN - 1] = '\0';
    }
    return 0;
}

// 지금 읽은 값으로 JSON을 만들고 기준값을 지금 값으로 바꾼다
static cJSON *build(MetricPoster *p, int64_t now) {
    cJSON *root = cJSON_CreateObject();
    cJSON *nodes = cJSON_AddArrayToObject(root, "nodes");
    cJSON *pods = cJSON_AddArrayToObject(root, "pods");
    cJSON_AddNumberToObject(root, "t", (double)now);
    size_t next_nodes = 0, next_pods = 0;

    for (size_t i = 0; i < p->node_count; i++) {
        const MetricPosterNode *n = &p->nodes[i];
        if (n->name[0] == '\0')
            continue;                              // 이름 없으면 대시보드에서 맞출 수 없음
        double cpu = NAN, drops = NAN, age = NAN, peak = NAN;
        if (n->valid && n->info.kind == SYSINFO_KIND_NODE) {
            const Prev *prev = find_prev(p->prev_nodes, p->prev_node_count, n->name);
            node_values(&n->info, prev ? &prev->info : NULL, &cpu, &drops);
            peak = peak_of(n->cpu_peak_pct, cpu);
            age = age_ms(&n->info, now);
            Prev *nx = &p->next_nodes[next_nodes++];
            memcpy(nx->name, n->name, sizeof(nx->name));
            nx->info = n->info;
        }
        cJSON *o = cJSON_CreateObject();
        cJSON_AddStringToObject(o, "name", n->name);
        add_num(o, "cpuPct", cpu);
        add_num(o, "cpuPeakPct", peak);
        add_num(o, "qdiscDropsPerSec", drops);
        add_num(o, "ageMs", age);
        cJSON_AddItemToArray(nodes, o);
    }

    for (size_t i = 0; i < p->pod_count; i++) {
        const MetricPosterPod *d = &p->pods[i];
        if (d->name[0] == '\0')
            continue;
        double cpu = NAN, thr_ms = NAN, thr_pct = NAN, age = NAN, peak = NAN;
        if (d->valid && d->info.kind == SYSINFO_KIND_CONTAINER) {
            const Prev *prev = find_prev(p->prev_pods, p->prev_pod_count, d->name);
            pod_values(&d->info, prev ? &prev->info : NULL, &cpu, &thr_ms, &thr_pct);
            peak = peak_of(d->cpu_peak_pct, cpu);
            age = age_ms(&d->info, now);
            Prev *nx = &p->next_pods[next_pods++];
            memcpy(nx->name, d->name, sizeof(nx->name));
            nx->info = d->info;
        }
        cJSON *o = cJSON_CreateObject();
        cJSON_AddStringToObject(o, "name", d->name);
        if (d->node[0])
            cJSON_AddStringToObject(o, "node", d->node);
        else
            cJSON_AddNullToObject(o, "node");
        add_num(o, "cpuPct", cpu);
        add_num(o, "cpuPeakPct", peak);
        add_num(o, "throttledMs", thr_ms);
        add_num(o, "throttledPct", thr_pct);
        add_num(o, "ageMs", age);
        cJSON_AddItemToArray(pods, o);
    }

    // 보내기 성공 여부와 상관없이 기준값 갱신
    memcpy(p->prev_nodes, p->next_nodes, next_nodes * sizeof(Prev));
    memcpy(p->prev_pods, p->next_pods, next_pods * sizeof(Prev));
    p->prev_node_count = next_nodes;
    p->prev_pod_count = next_pods;
    return root;
}

char *metric_poster_build_json(MetricPoster *p) {
    if (read_slots(p) != 0)
        return NULL;
    cJSON *root = build(p, now_ms());
    char *s = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    return s;
}

// ---- POST ----

static size_t write_cb(char *data, size_t size, size_t nmemb, void *user) {
    MetricPoster *p = user;
    size_t n = size * nmemb;
    size_t room = RESP_MAX - 1 - p->resp_len;
    size_t take = n < room ? n : room;
    memcpy(p->resp + p->resp_len, data, take);
    p->resp_len += take;
    p->resp[p->resp_len] = '\0';
    return n;                                      // 다 받은 것으로 (안 그러면 curl이 에러 처리)
}

static int post(MetricPoster *p, const char *body, char *err, size_t errlen) {
    p->resp_len = 0;
    p->resp[0] = '\0';
    curl_easy_setopt(p->curl, CURLOPT_POSTFIELDS, body);
    curl_easy_setopt(p->curl, CURLOPT_POSTFIELDSIZE, (long)strlen(body));

    CURLcode res = curl_easy_perform(p->curl);
    if (res != CURLE_OK) {
        snprintf(err, errlen, "%s", curl_easy_strerror(res));
        return -1;
    }
    long code = 0;
    curl_easy_getinfo(p->curl, CURLINFO_RESPONSE_CODE, &code);
    if (code != 200) {
        snprintf(err, errlen, "HTTP %ld %s", code, p->resp);
        return -1;
    }
    return 0;
}

// 상태가 바뀔 때만 로그 (매초 실패 로그가 쌓이지 않게)
static void report(MetricPoster *p, int ok, const char *err, int64_t now) {
    if (ok) {
        if (p->state == 0)
            fprintf(stderr, "metric_poster: 대시보드 전송 복구 (%lld초 동안 실패)\n",
                    (long long)((now - p->fail_since_ms) / 1000));
        else if (p->state == -1)
            printf("metric_poster: 대시보드 전송 시작 %s\n", p->cfg.url);
        p->state = 1;
    } else {
        if (p->state != 0) {
            fprintf(stderr, "metric_poster: 대시보드 전송 실패: %s (%s). 1초마다 계속 시도, 복구되면 알림\n",
                    err, p->cfg.url);
            p->fail_since_ms = now;
        }
        p->state = 0;
    }
}

int metric_poster_tick(MetricPoster *p) {
    int64_t now = now_ms();
    if (read_slots(p) != 0)
        return -1;
    cJSON *root = build(p, now);
    char *body = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    if (!body)
        return -1;

    char err[RESP_MAX + 64] = "";
    int rc = post(p, body, err, sizeof(err));
    report(p, rc == 0, err, now);
    if (p->cfg.debug)
        printf("[metric_poster] %s nodes=%zu pods=%zu %s\n", rc == 0 ? "ok" : "fail", p->node_count, p->pod_count, body);
    free(body);
    return rc;
}

// ---- 1초 주기 스레드 ----

static void *poster_main(void *arg) {
    MetricPoster *p = arg;
    while (atomic_load(&p->running)) {
        struct timespec wake;
        clock_gettime(CLOCK_REALTIME, &wake);
        wake.tv_sec += 1;                          // 다음 정각. 밀렸어도 몰아서 보내지 않고 다음 정각부터
        wake.tv_nsec = 0;
        int r;
        do {
            r = clock_nanosleep(CLOCK_REALTIME, TIMER_ABSTIME, &wake, NULL);
        } while (r == EINTR && atomic_load(&p->running));
        if (!atomic_load(&p->running))
            break;
        metric_poster_tick(p);
    }
    return NULL;
}

MetricPoster *metric_poster_create(const MetricPosterConfig *cfg, MetricPosterReadFn read, void *ctx) {
    if (!cfg || !read || cfg->url[0] == '\0') {
        fprintf(stderr, "metric_poster: url이 없음\n");
        return NULL;
    }
    MetricPoster *p = calloc(1, sizeof(*p));
    if (!p)
        return NULL;
    p->cfg = *cfg;
    p->cfg.url[sizeof(p->cfg.url) - 1] = '\0';
    if (p->cfg.timeout_ms <= 0)
        p->cfg.timeout_ms = DEFAULT_TIMEOUT_MS;
    p->read = read;
    p->ctx = ctx;
    p->state = -1;

    curl_global_init(CURL_GLOBAL_DEFAULT);         // 참조 카운트라 다른 곳에서 불렀어도 괜찮다
    p->curl = curl_easy_init();
    p->headers = curl_slist_append(NULL, "Content-Type: application/json");
    if (!p->curl || !p->headers) {
        fprintf(stderr, "metric_poster: curl 초기화 실패\n");
        metric_poster_destroy(p);
        return NULL;
    }
    // 핸들을 계속 재사용 → TCP 연결도 재사용. 끊기면 curl이 다음 요청에서 다시 연결한다
    curl_easy_setopt(p->curl, CURLOPT_URL, p->cfg.url);
    curl_easy_setopt(p->curl, CURLOPT_HTTPHEADER, p->headers);
    curl_easy_setopt(p->curl, CURLOPT_CONNECTTIMEOUT_MS,
                     (long)(p->cfg.timeout_ms < CONNECT_TIMEOUT_MS ? p->cfg.timeout_ms : CONNECT_TIMEOUT_MS));
    curl_easy_setopt(p->curl, CURLOPT_TIMEOUT_MS, (long)p->cfg.timeout_ms);
    curl_easy_setopt(p->curl, CURLOPT_NOSIGNAL, 1L);   // 멀티스레드에서 타임아웃에 시그널을 쓰지 않게
    curl_easy_setopt(p->curl, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(p->curl, CURLOPT_WRITEDATA, p);
    return p;
}

int metric_poster_start(MetricPoster *p) {
    if (p->thread_started)
        return 0;
    atomic_store(&p->running, 1);
    if (pthread_create(&p->thread, NULL, poster_main, p) != 0) {
        fprintf(stderr, "metric_poster: 스레드 시작 실패\n");
        atomic_store(&p->running, 0);
        return -1;
    }
    p->thread_started = 1;
    return 0;
}

void metric_poster_stop(MetricPoster *p) {
    if (!p || !p->thread_started)
        return;
    atomic_store(&p->running, 0);                  // 최대 1초(+전송 timeout) 안에 끝난다
    pthread_join(p->thread, NULL);
    p->thread_started = 0;
}

void metric_poster_destroy(MetricPoster *p) {
    if (!p)
        return;
    metric_poster_stop(p);
    if (p->curl)
        curl_easy_cleanup(p->curl);
    if (p->headers)
        curl_slist_free_all(p->headers);
    curl_global_cleanup();
    free(p);
}
