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
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <inttypes.h>  // PRIu64, PRIu32 매크로 사용을 위해 파일 상단에 추가

typedef struct {
    pthread_mutex_t mtx;
    SystemInfo info; // wrapper.h의 구조체를 그대로 사용
    int valid;
} MetricSlot;

typedef struct AppCollectorState AppCollectorState;
// 스트림 하나(=서버 하나)를 관리하는 단위
typedef struct {
    char ip[16];
    int port;
    MetricSlot *slot;    // 이 스트림이 값을 채워넣을 캐시 슬롯
    MetricSlot *node_slot; // 추가: pod면 소속 node를, node 자신이면 NULL
    void *stream_handle; // start_stream()이 리턴한 핸들 (정리용)
    FILE *log_file;
    AppCollectorState *owner;
} StreamEntry;

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

static void on_update(const SystemInfo *info, void *server_data) {
    StreamEntry *entry = (StreamEntry *)server_data;

    pthread_mutex_lock(&entry->slot->mtx);
    entry->slot->info = *info;
    entry->slot->valid = 1;
    pthread_mutex_unlock(&entry->slot->mtx);
    
    if(entry->owner->config.debug){
        printf("CPU 사용률: %.2f%% | 메모리 사용률: %.2f%% (%.1f MB / %.1f MB)", info->cpu_util_percent, info->mem_util_percent, info->mem_used_mb, info->mem_total_mb);
        printf("| Network tx: %d | Network drop: %d ", info->tx_bytes, info->tx_drop);    
        info->has_viewer_count ? printf("| 이용자 수: %d명 시청중\n", info->viewer_count) : printf("| [Node Data]\n");
    }
    
    if (!entry->log_file) return; // node 엔트리는 항상 log_file이 NULL이라 여기서 끝
    
    struct timeval tv;
    gettimeofday(&tv, NULL);
    long long ts_ms = (long long)tv.tv_sec * 1000 + tv.tv_usec / 1000;

    SystemInfo node_snapshot;
    int node_valid = 0;
    if (entry->node_slot) {
        pthread_mutex_lock(&entry->node_slot->mtx);
        node_valid = entry->node_slot->valid;
        if (node_valid) node_snapshot = entry->node_slot->info;
        pthread_mutex_unlock(&entry->node_slot->mtx);
    }

    if (node_valid) {
        fprintf(entry->log_file,
                "%lld,%.2f,%.2f,%" PRIu64 ",%" PRIu64 ",%" PRIu32 ",%.2f,%.2f,%" PRIu64 ",%" PRIu64 "\n",
                ts_ms,
                info->cpu_util_percent, info->mem_util_percent,
                info->tx_bytes, info->tx_drop,
                info->viewer_count,
                node_snapshot.cpu_util_percent, node_snapshot.mem_util_percent,
                node_snapshot.tx_bytes, node_snapshot.tx_drop);
    } else {
        fprintf(entry->log_file,
                "%lld,%.2f,%.2f,%" PRIu64 ",%" PRIu64 ",%" PRIu32 ",,,,\n",
                ts_ms,
                info->cpu_util_percent, info->mem_util_percent,
                info->tx_bytes, info->tx_drop,
                info->viewer_count);
    }
}

// node_key(IP)로 node_streams 배열에서 해당 node의 MetricSlot을 찾음.
// node 개수가 적다는 전제하에 선형 탐색 (app_init 시 1회만 호출되므로 핫패스 아님)
static MetricSlot *find_node_slot_by_ip(AppCollectorState *state, const char *node_ip) {
    for (size_t i = 0; i < state->node_count; i++) {
        if (strcmp(state->node_streams[i].ip, node_ip) == 0) {
            return state->node_streams[i].slot;
        }
    }
    return NULL;
}

// pod든 node든 공통인 "스트림 그룹 하나 초기화" 로직을 뽑아낸 헬퍼.
// list를 스냅샷 떠서 MetricSlot/StreamEntry 배열을 만들고 stream을 연다.
// open_log_files가 0이면 로그 파일을 열지 않음 (node용).
static int init_stream_group(AppCollectorState *state, ServerList *list,
                              MetricSlot **out_slots, StreamEntry **out_streams,
                              size_t *out_count, int open_log_files) {
    ServerSlot servers[MAX_SERVERS];
    size_t count = 0;
    server_list_snapshot(list, servers, MAX_SERVERS, &count);

    MetricSlot *slots = calloc(count, sizeof(MetricSlot));
    StreamEntry *streams = calloc(count, sizeof(StreamEntry));
    if (!slots || !streams) {
        free(slots);
        free(streams);
        return -1;
    }

    for (size_t i = 0; i < count; i++) {
        pthread_mutex_init(&slots[i].mtx, NULL);

        strncpy(streams[i].ip, servers[i].ip, sizeof(streams[i].ip) - 1);
        streams[i].ip[sizeof(streams[i].ip) - 1] = '\0';
        streams[i].port = servers[i].port;
        streams[i].slot = &slots[i];
        streams[i].node_slot = NULL; // pod면 나중에 app_init에서 따로 연결
        streams[i].log_file = NULL;
        streams[i].owner = state;

        if (open_log_files && state->save_metrics) {
            char path[256];
            time_t now = time(NULL);
            struct tm tm_buf;
            localtime_r(&now, &tm_buf);
            snprintf(path, sizeof(path), "./data/%s_%d_%04d%02d%02d_%02d%02d%02d.csv",
                    servers[i].ip, servers[i].port,
                    tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday,
                    tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec);

            streams[i].log_file = fopen(path, "w");
            if (streams[i].log_file) {
                fprintf(streams[i].log_file,
                        "timestamp,cpu_util,mem_util,tx_bytes,tx_drop,viewer_count,node_cpu_util,node_mem_util,node_tx_bytes,node_tx_drop\n");
            } else {
                fprintf(stderr, "app_collector: 로그 파일 열기 실패: %s\n", path);
            }
        }

        char addr[64];
        snprintf(addr, sizeof(addr), "%s:%d", streams[i].ip, streams[i].port);

        streams[i].stream_handle = start_stream(addr, on_update, &streams[i]);
        if (!streams[i].stream_handle) {
            fprintf(stderr, "app_collector: %s 스트림 시작 실패\n", addr);
        }
    }

    *out_slots = slots;
    *out_streams = streams;
    *out_count = count;
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

    // 1) node 먼저 초기화 (node는 로그 파일을 열지 않음)
    if (init_stream_group(state, state->config.node_server_list,
                           &state->node_slots, &state->node_streams,
                           &state->node_count, /*open_log_files=*/0) != 0) {
        return -1;
    }

    // 2) pod 초기화 (로그 파일 염)
    if (init_stream_group(state, state->config.pod_server_list,
                           &state->pod_slots, &state->pod_streams,
                           &state->pod_count, /*open_log_files=*/1) != 0) {
        return -1;
    }

    // 3) 각 pod에 대해 소속 node의 MetricSlot을 resolve해서 연결
    ServerSlot pod_servers[MAX_SERVERS];
    size_t pod_server_count = 0;
    server_list_snapshot(state->config.pod_server_list, pod_servers, MAX_SERVERS, &pod_server_count);

    for (size_t i = 0; i < state->pod_count && i < pod_server_count; i++) {
        MetricSlot *node_slot = find_node_slot_by_ip(state, pod_servers[i].node_key);
        if (!node_slot) {
            fprintf(stderr, "app_collector: pod %s 의 node(%s)를 찾을 수 없음\n",
                    pod_servers[i].ip, pod_servers[i].node_key);
        }
        state->pod_streams[i].node_slot = node_slot;
    }

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

        if (is_valid) {
            out[n++] = metric_create_system((int)i, temp_info.cpu_util_percent, temp_info.mem_util_percent);
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
