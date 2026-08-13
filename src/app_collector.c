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

/*typedef struct {
    AppCollectorConfig config;

    pthread_t *threads;
    int thread_count;

    pthread_mutex_t mutex;
    pthread_cond_t cond_start;
    pthread_cond_t cond_done;
    int generation;
    int active_workers;
    int shutdown;

    ServerSlot *cur_servers;
    size_t cur_server_count;
    atomic_size_t next_server_index;

    Metric *cur_out;
    size_t cur_max_count;
    atomic_size_t out_index;
} AppCollectorState;

static int fetch_metrics_from_server(const ServerSlot *server, const AppCollectorConfig *cfg, Metric *out_system,
                                     Metric *out_app_perf) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
        return -1;

    struct timeval timeout = {.tv_sec = 2, .tv_usec = 0};
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)cfg->server_list->servers->port);
    if (inet_pton(AF_INET, server->ip, &addr.sin_addr) != 1) {
        close(sock);
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(sock);
        return -1;
    }

    char request[256];
    int req_len = snprintf(request, sizeof(request), "GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n",
                           cfg->metrics_path, server->ip);
    if (req_len < 0 || write(sock, request, (size_t)req_len) < 0) {
        close(sock);
        return -1;
    }

    char response[4096];
    ssize_t total = 0;
    ssize_t n;
    while ((n = read(sock, response + total, sizeof(response) - (size_t)total - 1)) > 0) {
        total += n;
        if (total >= (ssize_t)sizeof(response) - 1)
            break;
    }
    close(sock);

    if (total <= 0)
        return -1;
    response[total] = '\0';

    // TODO: response를 실제로 파싱해서 아래 값들을 채우기
    *out_system = metric_create_system(0, 0.0, 0.0, "", 0.0);
    *out_app_perf = metric_create_app_perf(0, 0, 0.0, 0.0);

    return 0;
}

static void *worker_main(void *arg) {
    AppCollectorState *pool = (AppCollectorState *)arg;
    int cur_gen = 0;

    pthread_mutex_lock(&pool->mutex);
    for (;;) {
        while (pool->generation == cur_gen && !pool->shutdown) {
            pthread_cond_wait(&pool->cond_start, &pool->mutex);
        }
        if (pool->shutdown) {
            pthread_mutex_unlock(&pool->mutex);
            return NULL;
        }
        cur_gen = pool->generation;
        pthread_mutex_unlock(&pool->mutex);

        for (;;) {
            size_t idx = atomic_fetch_add(&pool->next_server_index, 1);
            if (idx >= pool->cur_server_count)
            break;
            if (atomic_load(&pool->cur_servers[idx].status) != SERVER_STATUS_UP)
            continue;

            //server에 접근해서 metric을 수집해옴(fetch)
            Metric sys_m, perf_m;
            if (fetch_metrics_from_server(&pool->cur_servers[idx], &pool->config, &sys_m, &perf_m) == 0) {
                size_t out_idx = atomic_fetch_add(&pool->out_index, 2);
                if (out_idx + 1 < pool->cur_max_count) {
                    pool->cur_out[out_idx] = sys_m;
                    pool->cur_out[out_idx + 1] = perf_m;
                }
            }
        }

        pthread_mutex_lock(&pool->mutex);
        pool->active_workers--;
        if (pool->active_workers == 0) {
            pthread_cond_signal(&pool->cond_done);
        }
    }
}*/

typedef struct {
    pthread_mutex_t mtx;
    SystemInfo info; // wrapper.h의 구조체를 그대로 사용
    int valid;
} MetricSlot;

// 스트림 하나(=서버 하나)를 관리하는 단위
typedef struct {
    char ip[16];
    int port;
    MetricSlot *slot;    // 이 스트림이 값을 채워넣을 캐시 슬롯
    void *stream_handle; // start_stream()이 리턴한 핸들 (정리용)
} StreamEntry;

typedef struct {
    AppCollectorConfig config;
    size_t server_count;

    MetricSlot *slots;    // 서버별 최신 metric 캐시 (mutex 포함)
    StreamEntry *streams; // 서버별 stream_handle 등 관리용
    pthread_mutex_t streams_mutex;

    int shutdown; // 종료 중인지 플래그 (재연결 여부 판단용)
} AppCollectorState;

static void on_update(const SystemInfo *info, void *server_data) {
    StreamEntry *entry = (StreamEntry *)server_data;

    pthread_mutex_lock(&entry->slot->mtx);
    entry->slot->info = *info;

    printf("CPU 사용률: %.2f%% | 메모리 사용률: %.2f%% (%.1f MB / %.1f MB) | 이용자 수: %d명 시청중\n", info->cpu_util_percent,
           info->mem_util_percent, info->mem_used_mb, info->mem_total_mb);
    
    info->has_viewer_count ? printf("| 이용자 수: %d명 시청중\n", info->viewer_count) : printf("| [Node Data]\n");
    entry->slot->valid = 1;
    pthread_mutex_unlock(&entry->slot->mtx);
}

static int app_init(Collector *self) {
    AppCollectorState *state = (AppCollectorState *)self->impl_data;
    pthread_mutex_init(&state->streams_mutex, NULL);
    //server_list_init(state->config.server_list);
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
