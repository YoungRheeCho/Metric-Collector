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
    void *stream_handle; // start_stream()이 리턴한 핸들 (정리용)
    FILE *log_file;
    AppCollectorState *owner;
} StreamEntry;

struct AppCollectorState{
    AppCollectorConfig config;
    size_t server_count;

    MetricSlot *slots;    // 서버별 최신 metric 캐시 (mutex 포함)
    StreamEntry *streams; // 서버별 stream_handle 등 관리용
    pthread_mutex_t streams_mutex;

    int save_metrics;
    int shutdown; // 종료 중인지 플래그 (재연결 여부 판단용)
} ;

static void on_update(const SystemInfo *info, void *server_data) {
    StreamEntry *entry = (StreamEntry *)server_data;

    pthread_mutex_lock(&entry->slot->mtx);
    entry->slot->info = *info;
    entry->slot->valid = 1;
    if(entry->owner->config.debug){
        printf("CPU 사용률: %.2f%% | 메모리 사용률: %.2f%% (%.1f MB / %.1f MB)", info->cpu_util_percent, info->mem_util_percent, info->mem_used_mb, info->mem_total_mb);    
        info->has_viewer_count ? printf("| 이용자 수: %d명 시청중\n", info->viewer_count) : printf("| [Node Data]\n");
    }
    
    pthread_mutex_unlock(&entry->slot->mtx);

    if (entry->log_file) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        long long ts_ms = (long long)tv.tv_sec * 1000 + tv.tv_usec / 1000;

        if (info->has_viewer_count) {
            fprintf(entry->log_file, "%lld,%.2f,%.2f,%d\n",
                    ts_ms, info->cpu_util_percent, info->mem_util_percent, info->viewer_count);
        } else {
            fprintf(entry->log_file, "%lld,%.2f,%.2f,\n",
                    ts_ms, info->cpu_util_percent, info->mem_util_percent);
        }
    }
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

    ServerSlot servers[MAX_SERVERS];
    size_t server_count = 0;
    server_list_snapshot(state->config.server_list, servers, MAX_SERVERS, &server_count);

    state->slots = calloc(server_count, sizeof(MetricSlot));
    state->streams = calloc(server_count, sizeof(StreamEntry));
    if (!state->slots || !state->streams)
        return -1;
    state->server_count = server_count;

    for (size_t i = 0; i < server_count; i++) {
        pthread_mutex_init(&state->slots[i].mtx, NULL);

        strncpy(state->streams[i].ip, servers[i].ip, sizeof(state->streams[i].ip));
        state->streams[i].port = servers[i].port;
        state->streams[i].slot = &state->slots[i];
        state->streams[i].log_file = NULL;
        state->streams[i].owner = state;

        if (state->save_metrics) {
            char path[256];
            time_t now = time(NULL);
            struct tm tm_buf;
            localtime_r(&now, &tm_buf);
            snprintf(path, sizeof(path), "./data/%s_%d_%04d%02d%02d_%02d%02d%02d.csv",
                    servers[i].ip, servers[i].port,
                    tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday,
                    tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec);

            state->streams[i].log_file = fopen(path, "w");
            if (state->streams[i].log_file) {
                fprintf(state->streams[i].log_file, "timestamp,cpu_util,mem_util,viewer_count\n");
            } else {
                fprintf(stderr, "app_collector: 로그 파일 열기 실패: %s\n", path);
            }
        }

        char addr[64];
        snprintf(addr, sizeof(addr), "%s:%d", state->streams[i].ip, state->streams[i].port);

        state->streams[i].stream_handle = start_stream(addr, on_update, &state->streams[i]);
        if (!state->streams[i].stream_handle) {
            fprintf(stderr, "app_collector: %s 스트림 시작 실패\n", addr);
        }
    }

    return 0;
}

static int app_collect(Collector *self, Metric *out, size_t max_count, size_t *out_count) {
    AppCollectorState *state = (AppCollectorState *)self->impl_data;
    size_t n = 0;

    ServerSlot snapshot[MAX_SERVERS];
    size_t server_count = 0;
    server_list_snapshot(state->config.server_list, snapshot, MAX_SERVERS, &server_count);

    pthread_mutex_lock(&state->streams_mutex);

    for (size_t i = 0; i < state->server_count && n < max_count; i++) {
        int is_up = 0;
        for (size_t j = 0; j < server_count; j++) {
            if (strcmp(snapshot[j].ip, state->streams[i].ip) == 0 && snapshot[j].port == state->streams[i].port &&
                atomic_load(&snapshot[j].status) == SERVER_STATUS_UP) {
                is_up = 1;
                break;
            }
        }
        if (!is_up)
            continue;

        SystemInfo temp_info;
        int is_valid;
        pthread_mutex_lock(&state->slots[i].mtx);
        is_valid = state->slots[i].valid;
        if (is_valid)
            temp_info = state->slots[i].info;
        pthread_mutex_unlock(&state->slots[i].mtx);

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
