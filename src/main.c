#include <dirent.h>
#include <getopt.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "app_collector.h"
#include "channel.h"
#include "channel_shm.h"
#include "config.h"
#include "metric.h"
#include "shared_types.h"

#define MAX_METRICS_PER_CYCLE (MAX_SERVERS * 2) /* 서버당 SystemMetric + AppPerfMetric */
#define SHM_DIR "/dev/shm"
#define BACKEND_PREFIX "n2sl_backend_"
#define BACKEND_PREFIX_LEN 13 /* strlen("n2sl_backend_") */
#define MAX_BACKENDS 32

static volatile sig_atomic_t running = 1;

static void handle_signal(int sig) {
    (void)sig;
    running = 0;
}

int discover_n2sl_backends(char out_names[][64], int max_backends) {
    DIR *d = opendir(SHM_DIR);
    if (!d) {
        perror("discover_n2sl_backends: opendir");
        return -1;
    }

    int count = 0;
    struct dirent *entry;
    while ((entry = readdir(d)) != NULL && count < max_backends) {
        if (strncmp(entry->d_name, BACKEND_PREFIX, BACKEND_PREFIX_LEN) != 0) {
            continue;
        }
        snprintf(out_names[count], 64, "/%s", entry->d_name);
        count++;
    }

    closedir(d);
    return count;
}

int main(int argc, char *argv[]) {
    const char *config_path = NULL;

    static struct option long_options[] = {
        {"config", required_argument, NULL, 'c'}, {"help", no_argument, NULL, 'h'}, {NULL, 0, NULL, 0}};

    int opt;
    while ((opt = getopt_long(argc, argv, "c:h", long_options, NULL)) != -1) {
        switch (opt) {
            case 'c':
                config_path = optarg;
                break;
            case 'h':
                printf("사용법: %s -c <config file>\n", argv[0]);
                return 0;
            default:
                fprintf(stderr, "사용법: %s -c <config file>\n", argv[0]);
                return 1;
        }
    }

    if (config_path == NULL) {
        fprintf(stderr, "에러: config 파일 경로가 필요합니다 (-c 또는 --config)\n");
        return 1;
    }

    Config cfg;
    if (config_load(config_path, &cfg) != 0) {
        fprintf(stderr, "에러: config 파일을 읽을 수 없습니다: %s\n", config_path);
        return 1;
    }

    // HAProxy 서버 목록 채널: 이미 존재하는 shared memory에서 읽어옴
    Channel *server_source =
        shm_channel_create(cfg.haproxy_shm_name, sizeof(ServerSlot), MAX_SERVERS, 0, 0);
    if (!server_source || server_source->init(server_source) != 0) {
        fprintf(stderr, "에러: HAProxy shared memory에 연결할 수 없습니다 (%s)\n",
                cfg.haproxy_shm_name);
        return 1;
    }

    ServerList g_server_list;
    server_list_init(&g_server_list);

    RefresherArgs refresher_args = {
        .channel = server_source,
        .list = &g_server_list,
        .interval_sec = cfg.refresh_interval_sec,
        .running = &running,
    };
    pthread_t refresher_thread;
    if (pthread_create(&refresher_thread, NULL, refresher_main, &refresher_args) != 0) {
        fprintf(stderr, "에러: 갱신 스레드 생성 실패\n");
        server_source->close(server_source);
        return 1;
    }

    //refactoring: backend 여러 개를 사용하게 되면 collector N개, shm N개를 사용해야하므로 수정이 필요함
    /*char backend_shm_names[MAX_BACKENDS][64];
    int backend_count = discover_n2sl_backends(backend_shm_names, MAX_BACKENDS);
    if (backend_count <= 0) {
        fprintf(stderr, "ERROR: n2sl backend shm을 하나도 못 찾음\n");
        return 1;
    }

    ServerList server_lists[MAX_BACKENDS];
    Channel *channels[MAX_BACKENDS];
    Collector *collectors[MAX_BACKENDS];
    pthread_t refresher_threads[MAX_BACKENDS];
    RefresherArgs refresher_args[MAX_BACKENDS];

    for (int i = 0; i < backend_count; i++) {
        server_list_init(&server_lists[i]);

        channels[i] = shm_channel_create(backend_shm_names[i], sizeof(ServerSlot), MAX_SERVERS, 0, 0);
        channels[i]->init(channels[i]);

        refresher_args[i] = (RefresherArgs){channels[i], &server_lists[i], cfg.refresh_interval_sec, &running};
        pthread_create(&refresher_threads[i], NULL, refresher_main, &refresher_args[i]);

        AppCollectorConfig app_cfg = {.server_list = &server_lists[i], .worker_pool_size = cfg.worker_pool_size};
        collectors[i] = app_collector_create(&app_cfg);
        collectors[i]->init(collectors[i]);
    }*/

    // MLP 출력 채널
    /*Channel *mlp_sink = shm_channel_create(cfg.mlp_shm_name, sizeof(Metric),
                                           cfg.mlp_ring_capacity, 1, 1);
    if (!mlp_sink || mlp_sink->init(mlp_sink) != 0) {
        fprintf(stderr, "에러: MLP shared memory를 생성할 수 없습니다 (%s)\n",
                cfg.mlp_shm_name);
        server_source->close(server_source);
        return 1;
    }*/

    AppCollectorConfig app_cfg = {
        .server_list = &g_server_list,
        .worker_pool_size = cfg.worker_pool_size,
    };
    strncpy(app_cfg.metrics_path, cfg.metrics_path, sizeof(app_cfg.metrics_path) - 1);

    Collector *collector = app_collector_create(&app_cfg);
    /*if (!collector || collector->init(collector) != 0) {
        fprintf(stderr, "에러: collector 초기화 실패\n");
        mlp_sink->close(mlp_sink);
        server_source->close(server_source);
        return 1;
    }*/

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    static Metric buf[MAX_METRICS_PER_CYCLE];

    while (running) {
        size_t count = 0;
        if (collector->collect(collector, buf, MAX_METRICS_PER_CYCLE, &count) == 0 && count > 0) {
            // mlp_sink->write(mlp_sink, buf, count);
        }
        sleep((unsigned int)cfg.collect_interval_sec);
    }

    collector->destroy(collector);
    server_source->close(server_source);
    // mlp_sink->close(mlp_sink);
    return 0;
}
