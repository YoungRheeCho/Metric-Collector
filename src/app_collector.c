#include "app_collector.h"
#include "shared_types.h"
#include "wrapper.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <inttypes.h>  // PRIu64, PRIu32 매크로 사용을 위해 파일 상단에 추가
#include <math.h>      // NAN, isnan (계산 불가한 값 표시)

typedef struct {            // node 스트림의 on_update에서 직전 node 샘플과의 Δ로 계산 (NAN = 계산 불가)
    double tx_bytes;        // 구간 동안 늘어난 물리 NIC 송신 바이트
    double tx_packets;      // 구간 동안 늘어난 물리 NIC 송신 패킷
    double qdisc_drops;     // 구간 동안 늘어난 qdisc drop
    double cpu_pct;         // 구간 동안의 node CPU 사용률 (%) = 1 - Δidle/Δtotal (대시보드 CPU 최대용)
} NodeRates;

typedef struct {
    pthread_mutex_t mtx;
    SystemInfo info; // wrapper.h의 구조체를 그대로 사용
    NodeRates node_rates; // node 슬롯에서만 사용
    int valid;
    double cpu_peak_pct;  // 직전 app_collector_read 이후 0.1초 구간 CPU 사용률의 최대 (NAN = 아직 없음). read가 비운다
} MetricSlot;

typedef struct AppCollectorState AppCollectorState;
// 스트림 하나(=서버 하나)를 관리하는 단위
typedef struct {
    char ip[16];
    char name[64];
    char node_name[64];
    char node_key[16]; //pod가 속한 node의 IP (sync 때마다 node 다시 연결용)
    int port;
    int active; // 1 = 사용 중인 칸, 0 = 빈 칸
    MetricSlot *slot;    // 이 스트림이 값을 채워넣을 캐시 슬롯
    MetricSlot *node_slot; // 추가: pod면 소속 node를, node 자신이면 NULL
    void *stream_handle; // start_stream()이 리턴한 핸들 (정리용)
    FILE *log_file;
    AppCollectorState *owner;
    SystemInfo prev;     // 직전 샘플 (Δ 계산용). on_update는 스트림마다 하나씩 순서대로 불리므로 락 없이 사용
    int has_prev;
    LbPod *lb_pod;
    int fail_count;      // 연속 연결 실패 횟수 (값을 받은 스트림이 끊기면 0부터 다시)
    time_t retry_at;     // 다음 재연결 시각 (monotonic 초). 0 = 끊김을 아직 발견 안 함
} StreamEntry;

#define RECONNECT_MAX_DELAY_SEC 30

struct AppCollectorState{
    AppCollectorConfig config;
    size_t server_count;

    size_t pod_count;
    MetricSlot *pod_slots;    // 서버별 최신 metric 캐시 (mutex 포함)
    StreamEntry *pod_streams; // 서버별 stream_handle 등 관리용

    size_t node_count;
    MetricSlot *node_slots;
    StreamEntry *node_streams;

    pthread_mutex_t streams_mutex;

    int save_metrics;
    int shutdown; // 종료 중인지 플래그 (재연결 여부 판단용)
} ;

typedef struct {            // container 행에서 직전 container 샘플과의 Δ로 계산 (NAN = 계산 불가)
    double dt_ms;           // 직전 샘플과의 시간 차
    double cpu_util_pct;    // 한도 대비 CPU 사용률 (%) = Δusage_usec / Δt(us) / limit_cores * 100
    double thr_count;       // 구간 동안 늘어난 nr_throttled
    double thr_usec;        // 구간 동안 늘어난 throttled_usec
} ContainerRates;

// 누적값의 증가량. 값이 줄었으면(리셋) NAN
static double delta(uint64_t cur, uint64_t prev) {
    if (cur < prev) return NAN;
    return (double)(cur - prev);
}

static NodeRates calc_node_rates(const SystemInfo *prev, int has_prev, const SystemInfo *cur) {
    NodeRates r = { NAN, NAN, NAN, NAN };
    if (!has_prev) return r;
    
    const NodeInfo *p = &prev->node;
    const NodeInfo *c = &cur->node;
    
    if (p->has_net && c->has_net) {
        r.tx_bytes = delta(c->net.tx_bytes, p->net.tx_bytes);
        r.tx_packets = delta(c->net.tx_packets, p->net.tx_packets);
    }
    if (p->has_qdisc && c->has_qdisc)
        r.qdisc_drops = (uint32_t)(c->qdisc_drops - p->qdisc_drops);   // 32비트 넘침은 uint32 뺄셈으로 처리
        if (p->has_cpu && c->has_cpu) {
            int64_t dtotal = c->cpu_total - p->cpu_total, didle = c->cpu_idle - p->cpu_idle;
            if (dtotal > 0 && didle >= 0 && didle <= dtotal)   // 0.1초 동안 jiffies가 안 늘었으면(dtotal==0) 계산 안 함
            r.cpu_pct = (1.0 - (double)didle / (double)dtotal) * 100.0;
        }
        return r;
    }
    
    static ContainerRates calc_container_rates(const SystemInfo *prev, int has_prev, const SystemInfo *cur) {
        ContainerRates r = { NAN, NAN, NAN, NAN };
        if (!has_prev) return r;

        const ContainerInfo *p = &prev->container;
    const ContainerInfo *c = &cur->container;
    r.dt_ms = (double)(cur->recv_ms - prev->recv_ms);

    if (r.dt_ms <= 0 || !p->has_cpu || !c->has_cpu) return r;
    if (c->cpu_usage_usec < p->cpu_usage_usec || c->cpu_nr_throttled < p->cpu_nr_throttled ||
        c->cpu_throttled_usec < p->cpu_throttled_usec)
        return r;   // 리셋 (컨테이너 재시작, cgroup 재탐색)
        
        if (c->cpu_limit_cores > 0)   // 무제한(-1)이면 한도 대비 사용률은 계산 불가
        r.cpu_util_pct = (double)(c->cpu_usage_usec - p->cpu_usage_usec) / (r.dt_ms * 1000.0) / c->cpu_limit_cores * 100.0;
        r.thr_count = (double)(c->cpu_nr_throttled - p->cpu_nr_throttled);
        r.thr_usec = (double)(c->cpu_throttled_usec - p->cpu_throttled_usec);
        return r;
    }

    // CSV 한 칸: NAN이면 빈 칸
    static void csv_num(FILE *f, double v, int prec) {
        if (isnan(v)) fprintf(f, ",");
        else          fprintf(f, ",%.*f", prec, v);
    }

    // 디버그 출력 한 칸: NAN이면 "-"
    static void print_num(const char *label, double v, int prec, const char *unit) {
    if (isnan(v)) printf(" %s=-", label);
    else          printf(" %s=%.*f%s", label, prec, v, unit);
}

static void on_update(const SystemInfo *info, void *server_data) {
    StreamEntry *entry = (StreamEntry *)server_data;

    // 직전 샘플이 같은 종류일 때만 Δ 계산
    int has_prev = entry->has_prev && entry->prev.kind == info->kind;
    NodeRates node_rates = { NAN, NAN, NAN, NAN };
    ContainerRates rates = { NAN, NAN, NAN, NAN };
    if (info->kind == SYSINFO_KIND_NODE)
    node_rates = calc_node_rates(&entry->prev, has_prev, info);
    else if (info->kind == SYSINFO_KIND_CONTAINER)
    rates = calc_container_rates(&entry->prev, has_prev, info);

    if (entry->lb_pod && info->kind == SYSINFO_KIND_CONTAINER)
    lb_model_add_sample(entry->lb_pod, info->recv_ms, rates.cpu_util_pct, rates.thr_usec,
                        info->container.viewer_count, info->container.cpu_limit_cores);
    
                        pthread_mutex_lock(&entry->slot->mtx);
    entry->slot->info = *info;
    entry->slot->node_rates = node_rates;
    entry->slot->valid = 1;
    {   // 대시보드 "CPU 최대": 0.1초 구간 사용률의 최대를 쌓아 두고, poster가 1초마다 읽으면서 비운다
        double v = info->kind == SYSINFO_KIND_CONTAINER ? rates.cpu_util_pct : node_rates.cpu_pct;
        if (!isnan(v) && (isnan(entry->slot->cpu_peak_pct) || v > entry->slot->cpu_peak_pct))
        entry->slot->cpu_peak_pct = v;
    }
    pthread_mutex_unlock(&entry->slot->mtx);
    
    if (entry->owner->config.debug) {
        flockfile(stdout);   // 한 줄을 printf 여러 번으로 찍으므로, 다른 스레드 출력이 줄 중간에 끼지 않게
        if (info->kind == SYSINFO_KIND_CONTAINER) {
            const ContainerInfo *c = &info->container;
            printf("[container %s:%d] recv=%" PRId64, entry->ip, entry->port, info->recv_ms);
            print_num("dt", rates.dt_ms, 0, "ms");
            if (c->has_cpu) {
                printf(" | cpu");
                print_num("util", rates.cpu_util_pct, 2, "%");
                printf(" limit=%.2f |", c->cpu_limit_cores);
                print_num("thr_cnt", rates.thr_count, 0, "");
                print_num("thr_time", rates.thr_usec, 0, "us");
            } else {
                printf(" | cpu=none");
            }
            printf(" | viewers=%" PRIu32 "\n", c->viewer_count);
        } else if (info->kind == SYSINFO_KIND_NODE) {
            const NodeInfo *n = &info->node;
            printf("[node %s:%d] recv=%" PRId64 " |", entry->ip, entry->port, info->recv_ms);
            print_num("tx", node_rates.tx_bytes, 0, "B");
            print_num("tx_pkt", node_rates.tx_packets, 0, "pkt");
            if (n->has_qdisc) {
                printf(" | qdisc %s", n->qdisc_kind);
                print_num("drops", node_rates.qdisc_drops, 0, "");
                printf(" backlog=%" PRIu32 "B\n", n->qdisc_backlog);
            } else {
                printf(" | qdisc=none\n");
            }
        } else {
            printf("[%s:%d] kind=NONE (node/container 둘 다 없음, sysinfo 버전 확인 필요)\n", entry->ip, entry->port);
        }
        funlockfile(stdout);
    }
    
    entry->prev = *info;
    entry->has_prev = 1;
    
    if (!entry->log_file) return;                        // node 엔트리는 log_file이 NULL
    if (info->kind != SYSINFO_KIND_CONTAINER) return;    // pod 스트림인데 container 응답이 아니면 기록 안 함
    
    SystemInfo node_snapshot;
    NodeRates node_snapshot_rates = { NAN, NAN, NAN, NAN };
    int node_valid = 0;
    if (entry->node_slot) {
        pthread_mutex_lock(&entry->node_slot->mtx);
        node_valid = entry->node_slot->valid && entry->node_slot->info.kind == SYSINFO_KIND_NODE;
        if (node_valid) {
            node_snapshot = entry->node_slot->info;
            node_snapshot_rates = entry->node_slot->node_rates;
        }
        pthread_mutex_unlock(&entry->node_slot->mtx);
    }
    
    FILE *f = entry->log_file;
    const ContainerInfo *c = &info->container;
    
    // 계산값
    fprintf(f, "%" PRId64, info->recv_ms);
    csv_num(f, rates.dt_ms, 0);
    csv_num(f, rates.cpu_util_pct, 2);
    csv_num(f, rates.thr_count, 0);
    csv_num(f, rates.thr_usec, 0);
    csv_num(f, node_snapshot_rates.tx_bytes, 0);
    csv_num(f, node_snapshot_rates.tx_packets, 0);
    csv_num(f, node_snapshot_rates.qdisc_drops, 0);
    if (node_valid && node_snapshot.node.has_qdisc) fprintf(f, ",%" PRIu32, node_snapshot.node.qdisc_backlog);
    else                                            fprintf(f, ",");

    // 원본값
    if (c->has_cpu)
    fprintf(f, ",%" PRIu64 ",%.2f,%" PRIu64 ",%" PRIu64 ",%" PRIu64, c->cpu_usage_usec, c->cpu_limit_cores,
        c->cpu_nr_periods, c->cpu_nr_throttled, c->cpu_throttled_usec);
        else
        fprintf(f, ",,,,,");
        if (c->has_mem) fprintf(f, ",%.2f,%.2f", c->mem.used_mb, c->mem.total_mb);
        else            fprintf(f, ",,");
        if (c->has_net) fprintf(f, ",%" PRIu64 ",%" PRIu64 ",%" PRIu64, c->net.tx_bytes, c->net.tx_packets, c->net.tx_drop);
        else            fprintf(f, ",,,");
        fprintf(f, ",%" PRIu32, c->viewer_count);
        
        if (node_valid) {
            const NodeInfo *n = &node_snapshot.node;
            fprintf(f, ",%" PRId64, node_snapshot.recv_ms);
            if (n->has_cpu) fprintf(f, ",%" PRId64 ",%" PRId64, n->cpu_idle, n->cpu_total);
            else            fprintf(f, ",,");
        if (n->has_mem) fprintf(f, ",%.2f,%.2f", n->mem.used_mb, n->mem.total_mb);
        else            fprintf(f, ",,");
        if (n->has_net) fprintf(f, ",%" PRIu64 ",%" PRIu64 ",%" PRIu64, n->net.tx_bytes, n->net.tx_packets, n->net.tx_drop);
        else            fprintf(f, ",,,");
        if (n->has_qdisc)
        fprintf(f, ",%" PRIu64 ",%" PRIu32 ",%" PRIu32 ",%" PRIu32 ",%" PRIu32 ",%" PRIu32 ",%" PRIu32,
                    n->qdisc_bytes, n->qdisc_packets, n->qdisc_qlen, n->qdisc_backlog,
                    n->qdisc_drops, n->qdisc_requeues, n->qdisc_overlimits);
        else
        fprintf(f, ",,,,,,,");
    } else {
        fprintf(f, ",,,,,,,,,,,,,,,");                  // node 열 15개 빈 칸
    }
    fprintf(f, "\n");
    fflush(f);
}

// node_key(IP)로 node_streams 배열에서 해당 node의 StreamEntry를 찾음.
static StreamEntry *find_node_stream_by_ip(AppCollectorState *state, const char *node_ip) {
    for (size_t i = 0; i < state->node_count; i++) {
        if (state->node_streams[i].active && strcmp(state->node_streams[i].ip, node_ip) == 0) {
            return &state->node_streams[i];
        }
    }
    return NULL;
}

static void open_log_file(AppCollectorState *state, StreamEntry *e) {
    if (!state->save_metrics) return;

    char path[256];
    time_t now = time(NULL);
    struct tm tm_buf;
    localtime_r(&now, &tm_buf);
    snprintf(path, sizeof(path), "./data/%s_%d_%04d%02d%02d_%02d%02d%02d.csv",
             e->ip, e->port,
             tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday,
             tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec);

    e->log_file = fopen(path, "w");
    if (e->log_file) {
        fprintf(e->log_file,
            "recv_ms,dt_ms,cpu_util_pct,cpu_thr_count,cpu_thr_usec,node_tx_bytes_diff,node_tx_packets_diff,qdisc_drops_diff,node_qdisc_backlog,"
            "cpu_usage_usec,cpu_limit_cores,cpu_nr_periods,cpu_nr_throttled,cpu_throttled_usec,"
            "mem_used_mb,mem_total_mb,tx_bytes,tx_packets,tx_drop,viewer_count,"
            "node_recv_ms,node_cpu_idle,node_cpu_total,node_mem_used_mb,node_mem_total_mb,"
            "node_tx_bytes,node_tx_packets,node_tx_drop,"
            "qdisc_bytes,qdisc_packets,qdisc_qlen,qdisc_backlog,qdisc_drops,qdisc_requeues,qdisc_overlimits\n");
    } else {
        fprintf(stderr, "app_collector: 로그 파일 열기 실패: %s\n", path);
    }
}

// 서버 하나를 빈 칸에 넣고 스트림을 연다. is_pod=1이면 로그 파일, LB 등록까지
static void open_stream(AppCollectorState *state, MetricSlot *slots, StreamEntry *streams,
                        size_t *count, const ServerSlot *srv, int is_pod) {
    // 1) 빈 칸 찾기: 앞에서부터 active=0인 칸, 없으면 맨 뒤 새 칸
    size_t i = 0;
    while (i < *count && streams[i].active) i++;
    if (i >= MAX_SERVERS) {
        fprintf(stderr, "app_collector: MAX_SERVERS 초과, %s 무시\n", srv->name);
        return;
    }

    // 2) 칸 채우기 (아직 active=0이라 다른 스레드는 이 칸을 안 봄)
    StreamEntry *e = &streams[i];
    memset(e, 0, sizeof(*e));
    snprintf(e->ip, sizeof(e->ip), "%s", srv->ip);
    snprintf(e->name, sizeof(e->name), "%s", srv->name);
    snprintf(e->node_key, sizeof(e->node_key), "%s", srv->node_key);
    e->port = srv->port;
    e->slot = &slots[i];
    e->owner = state;

    pthread_mutex_lock(&slots[i].mtx);       // 예전 pod의 값이 남아 있을 수 있으니 비움
    slots[i].valid = 0;
    slots[i].cpu_peak_pct = NAN;
    pthread_mutex_unlock(&slots[i].mtx);

    if (is_pod) {
        open_log_file(state, e);
        if (state->config.lb_model)          // 실행 중에도 추가된다 (칸이 다 차면 NULL → LB 표에 안 들어감)
            e->lb_pod = lb_model_add_pod(state->config.lb_model, srv->ip);
    }

    // 3) 스트림 시작 (바로 리턴, 값은 gRPC 스레드가 on_update로 넣음)
    char addr[64];
    snprintf(addr, sizeof(addr), "%s:%d", e->ip, e->port);
    e->stream_handle = start_stream(addr, on_update, e);
    if (!e->stream_handle)
        fprintf(stderr, "app_collector: %s 스트림 시작 실패\n", addr);

    // 4) 다 채운 뒤에 "사용 중" 표시 (읽는 스레드들이 이때부터 이 칸을 봄)
    pthread_mutex_lock(&state->streams_mutex);
    e->active = 1;
    if (i == *count) (*count)++;
    pthread_mutex_unlock(&state->streams_mutex);

    printf("[sync] open %s %s\n", e->name, addr);
}

// 칸을 비운다. 순서가 중요: ① 안 보이게 → ② 스트림 정지(콜백 끝날 때까지 대기) → ③ 정리
static void close_stream(AppCollectorState *state, StreamEntry *e) {
    pthread_mutex_lock(&state->streams_mutex);
    e->active = 0;                              // ① poster, collect가 이 칸을 더 안 읽음
    pthread_mutex_unlock(&state->streams_mutex);

    stop_stream(e->stream_handle);              // ② 반환되면 on_update가 더 안 불림
    e->stream_handle = NULL;

    if (e->lb_pod) {                            // LB 표에서 이 pod를 "입력 없음"으로 (on_update가 멈춘 뒤라 안전)
        lb_model_remove_pod(state->config.lb_model, e->lb_pod);
        e->lb_pod = NULL;
    }

    if (e->log_file) {                          // ③ 이제 아무도 안 쓰니 안전하게 정리
        fclose(e->log_file);
        e->log_file = NULL;
    }
    pthread_mutex_lock(&e->slot->mtx);
    e->slot->valid = 0;
    pthread_mutex_unlock(&e->slot->mtx);

    printf("[sync] close %s %s\n", e->name, e->ip);
}

static time_t mono_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec;
}

// 끊긴 스트림을 같은 칸에서 다시 연다 (로그 파일, LB 등록은 그대로 유지)
static void reconnect_stream(StreamEntry *e) {
    stop_stream(e->stream_handle);   // 이미 끝난 상태라 바로 리턴
    e->has_prev = 0;                 // 끊긴 사이와 Δ를 계산하지 않게

    char addr[64];
    snprintf(addr, sizeof(addr), "%s:%d", e->ip, e->port);
    e->stream_handle = start_stream(addr, on_update, e);
}

// 스트림 상태 점검: 끊겼으면 backoff(1, 2, 4, ... 최대 30초)를 두고 재연결.
// 로그는 상태가 바뀔 때만 (처음 끊김 / 다시 연결됨) 출력한다
static void check_stream(StreamEntry *e, time_t now) {
    if (!stream_is_done(e->stream_handle)) {
        // 살아 있고 값도 받고 있음 → 실패 중이었다면 복구된 것
        if (e->fail_count > 0 && stream_received_any(e->stream_handle)) {
            printf("[stream] %s %s:%d 재연결됨 (%d회 실패 후)\n", e->name, e->ip, e->port, e->fail_count);
            e->fail_count = 0;
        }
        return;
    }

    if (e->retry_at == 0) {                       // 이번에 끊김을 처음 발견
        if (stream_received_any(e->stream_handle))
            e->fail_count = 0;                    // 잘 되던 스트림이 끊김 → backoff 처음부터
        e->fail_count++;

        int shift = e->fail_count - 1 < 5 ? e->fail_count - 1 : 5;
        int delay = 1 << shift;                   // 1, 2, 4, 8, 16, 32 → 최대값으로 자름
        if (delay > RECONNECT_MAX_DELAY_SEC) delay = RECONNECT_MAX_DELAY_SEC;
        e->retry_at = now + delay;

        if (e->fail_count == 1 || e->owner->config.debug) {
            char err[256];
            stream_last_error(e->stream_handle, err, sizeof(err));
            fprintf(stderr, "[stream] %s %s:%d 연결 끊김: %s (%d초 뒤 재시도%s)\n", e->name, e->ip, e->port,
                    err, delay, e->fail_count == 1 ? ", 이후 실패는 -d 일 때만 출력" : "");
        }
    }

    if (now >= e->retry_at) {
        reconnect_stream(e);
        e->retry_at = 0;
    }
}

// 목록에서 이름으로 서버 찾기
static const ServerSlot *find_server_by_name(const ServerSlot *list, size_t n, const char *name) {
    for (size_t i = 0; i < n; i++)
        if (strcmp(list[i].name, name) == 0) return &list[i];
    return NULL;
}

// 이 이름으로 사용 중인 스트림이 있는지
static int has_active_stream(const StreamEntry *streams, size_t count, const char *name) {
    for (size_t i = 0; i < count; i++)
        if (streams[i].active && strcmp(streams[i].name, name) == 0) return 1;
    return 0;
}

// 그룹 하나(node 또는 pod)를 server_list와 맞춘다
static void sync_group(AppCollectorState *state, ServerList *list, MetricSlot *slots,
                       StreamEntry *streams, size_t *count, int is_pod) {
    ServerSlot cur[MAX_SERVERS];
    size_t n = 0;
    server_list_snapshot(list, cur, MAX_SERVERS, &n);

    // 1) 지금 있는 스트림 점검
    time_t now = mono_sec();
    for (size_t i = 0; i < *count; i++) {
        StreamEntry *e = &streams[i];
        if (!e->active) continue;

        const ServerSlot *m = find_server_by_name(cur, n, e->name);
        if (!m || strcmp(m->ip, e->ip) != 0)
            close_stream(state, e);           // 목록에서 사라짐 / IP 바뀜 → 닫기
        else
            check_stream(e, now);             // 그대로인데 끊김 → backoff 후 재연결
    }

    // 2) 목록에 있는데 스트림이 없는 서버 → 열기 (IP 바뀐 pod도 1)에서 닫혔으니 여기서 새로 열림)
    for (size_t j = 0; j < n; j++)
        if (!has_active_stream(streams, *count, cur[j].name))
            open_stream(state, slots, streams, count, &cur[j], is_pod);
}

// main이 목록을 갱신한 뒤 부른다. node 먼저 → pod → pod와 node 연결
void app_collector_sync(Collector *self) {
    AppCollectorState *state = (AppCollectorState *)self->impl_data;

    sync_group(state, state->config.node_server_list, state->node_slots, state->node_streams,
               &state->node_count, /*is_pod=*/0);
    sync_group(state, state->config.pod_server_list, state->pod_slots, state->pod_streams,
               &state->pod_count, /*is_pod=*/1);

    // pod마다 소속 node를 다시 연결 (node가 새로 생겼거나 바뀌었을 수 있으므로 매번)
    for (size_t i = 0; i < state->pod_count; i++) {
        StreamEntry *p = &state->pod_streams[i];
        if (!p->active) continue;

        StreamEntry *node = find_node_stream_by_ip(state, p->node_key);
        p->node_slot = node ? node->slot : NULL;
        snprintf(p->node_name, sizeof(p->node_name), "%s", node ? node->name : "");
    }
}

int app_collector_read(void *ctx, MetricPosterNode *nodes, size_t max_nodes, size_t *nn,
                       MetricPosterPod *pods, size_t max_pods, size_t *np) {
    AppCollectorState *s = ((Collector *)ctx)->impl_data;

    pthread_mutex_lock(&s->streams_mutex);   // 읽는 동안 칸이 열리거나 닫히지 않게

    size_t n = 0;
    for (size_t i = 0; i < s->node_count && n < max_nodes; i++) {
        if (!s->node_streams[i].active) continue;          // 빈 칸 건너뜀
        snprintf(nodes[n].name, sizeof(nodes[n].name), "%s", s->node_streams[i].name);
        pthread_mutex_lock(&s->node_slots[i].mtx);
        nodes[n].valid = s->node_slots[i].valid;
        nodes[n].info = s->node_slots[i].info;
        nodes[n].cpu_peak_pct = s->node_slots[i].cpu_peak_pct;
        s->node_slots[i].cpu_peak_pct = NAN;   // read-and-reset: 다음 1초는 새로 잰다
        pthread_mutex_unlock(&s->node_slots[i].mtx);
        n++;
    }
    *nn = n;

    n = 0;
    for (size_t i = 0; i < s->pod_count && n < max_pods; i++) {
        if (!s->pod_streams[i].active) {
            continue;
        }
        snprintf(pods[n].name, sizeof(pods[n].name), "%s", s->pod_streams[i].name);
        snprintf(pods[n].node, sizeof(pods[n].node), "%s", s->pod_streams[i].node_name);
        pthread_mutex_lock(&s->pod_slots[i].mtx);
        pods[n].valid = s->pod_slots[i].valid;
        pods[n].info = s->pod_slots[i].info;
        pods[n].cpu_peak_pct = s->pod_slots[i].cpu_peak_pct;
        s->pod_slots[i].cpu_peak_pct = NAN;
        pthread_mutex_unlock(&s->pod_slots[i].mtx);
        n++;
    }
    *np = n;

    pthread_mutex_unlock(&s->streams_mutex);
    return 0;
}

static int app_init(Collector *self) {
    AppCollectorState *state = (AppCollectorState *)self->impl_data;
    state->save_metrics = state->config.save_metrics;
    pthread_mutex_init(&state->streams_mutex, NULL);
    //server_list_init(state->config.server_list);

    // 저장 모드면, 서버별 파일 열기 전에 디렉토리부터 확보
    if (state->save_metrics) {
        if (mkdir("data", 0755) != 0 && errno != EEXIST) {
            fprintf(stderr, "app_collector: 로그 디렉토리 생성 실패 (%s): %s\n",
                    "data", strerror(errno));
            state->save_metrics = 0;  // 디렉토리 확보 실패 시 저장 자체를 끄고 나머지는 정상 진행
        }
    }

    // 배열은 처음부터 최대 크기로 잡는다 (칸을 켜고 끄며 재사용, 해제하지 않음)
    state->node_slots   = calloc(MAX_SERVERS, sizeof(MetricSlot));
    state->node_streams = calloc(MAX_SERVERS, sizeof(StreamEntry));
    state->pod_slots    = calloc(MAX_SERVERS, sizeof(MetricSlot));
    state->pod_streams  = calloc(MAX_SERVERS, sizeof(StreamEntry));
    if (!state->node_slots || !state->node_streams || !state->pod_slots || !state->pod_streams) {
        fprintf(stderr, "app_collector: 메모리 할당 실패\n");
        return -1;
    }

    // 슬롯 mutex는 여기서 한 번만 init (칸을 재사용할 때 다시 init하지 않는다)
    for (size_t i = 0; i < MAX_SERVERS; i++) {
        pthread_mutex_init(&state->node_slots[i].mtx, NULL);
        pthread_mutex_init(&state->pod_slots[i].mtx, NULL);
    }
    state->node_count = 0;
    state->pod_count = 0;

    // 처음 목록으로 스트림 열기: 배열이 비어 있으니 목록 전부가 "새 서버"로 열림
    // (node 열기 → pod 열기 → pod와 node 연결까지 sync 안에서 함)
    app_collector_sync(self);

    return 0;
}



static int app_collect(Collector *self, Metric *out, size_t max_count, size_t *out_count) {
    AppCollectorState *state = (AppCollectorState *)self->impl_data;
    size_t n = 0;

    ServerSlot snapshot[MAX_SERVERS];
    size_t server_count = 0;
    server_list_snapshot(state->config.pod_server_list, snapshot, MAX_SERVERS, &server_count);

    pthread_mutex_lock(&state->streams_mutex);

    for (size_t i = 0; i < state->pod_count && n < max_count; i++) {
        if (!state->pod_streams[i].active) continue;
        int is_up = 0;
        for (size_t j = 0; j < server_count; j++) {
            if (strcmp(snapshot[j].ip, state->pod_streams[i].ip) == 0 && snapshot[j].port == state->pod_streams[i].port &&
                atomic_load(&snapshot[j].status) == SERVER_STATUS_UP) {
                is_up = 1;
                break;
            }
        }
        if (!is_up)
            continue;

        SystemInfo temp_info;
        int is_valid;
        pthread_mutex_lock(&state->pod_slots[i].mtx);
        is_valid = state->pod_slots[i].valid;
        if (is_valid)
            temp_info = state->pod_slots[i].info;
        pthread_mutex_unlock(&state->pod_slots[i].mtx);

        if (is_valid && temp_info.kind == SYSINFO_KIND_CONTAINER) {
            const ContainerInfo *c = &temp_info.container;
            double mem_util = (c->has_mem && c->mem.total_mb > 0) ? c->mem.used_mb / c->mem.total_mb * 100.0 : -1.0;
            out[n++] = metric_create_system((int)i, -1.0 /* TODO: CPU 사용률은 Δ 계산 후 */, mem_util);
        }
    }

    pthread_mutex_unlock(&state->streams_mutex);
    *out_count = n;
    return 0;
}

static void app_destroy(Collector *self) {
    /*AppCollectorState *state = (AppCollectorState *)self->impl_data;

    pthread_mutex_lock(&state->mutex);
    state->shutdown = 1;
    state->generation++;
    pthread_cond_broadcast(&state->cond_start);
    pthread_mutex_unlock(&state->mutex);

    for (int i = 0; i < state->thread_count; i++) {
        pthread_join(state->threads[i], NULL);
    }

    free(state->threads);
    pthread_mutex_destroy(&state->mutex);
    pthread_cond_destroy(&state->cond_start);
    pthread_cond_destroy(&state->cond_done);

    free(state);
    free(self);*/
}

Collector *app_collector_create(const AppCollectorConfig *config) {
    AppCollectorState *state = malloc(sizeof(AppCollectorState));
    if (!state)
        return NULL;
    memset(state, 0, sizeof(*state));
    state->config = *config;

    Collector *c = malloc(sizeof(Collector));
    if (!c) {
        free(state);
        return NULL;
    }
    c->collector_id = "app_collector";
    c->init = app_init;
    c->collect = app_collect;
    c->flush = NULL;
    c->destroy = app_destroy;
    c->impl_data = state;
    return c;
}
