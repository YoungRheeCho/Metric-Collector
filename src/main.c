#include <dirent.h>
#include <getopt.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#include "app_collector.h"
#include "channel.h"
#include "channel_shm.h"
#include "config.h"
#include "metric.h"
#include "shared_types.h"
#include "k8s_creds.h"
#include "k8s_discovery.h"

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
    setvbuf(stdout, NULL, _IOLBF, 0);   // docker(pipe)에서도 줄 단위로 출력 (stderr와 줄 중간에서 섞이지 않게)

    const char *config_path = NULL;

    static struct option long_options[] = {
        {"config", required_argument, NULL, 'c'},
        {"save-metrics", no_argument, NULL, 's'},
        {"debug", no_argument, NULL, 'd'},
        {"lb-model", required_argument, NULL, 'm'},
        {"help", no_argument, NULL, 'h'},
        {NULL, 0, NULL, 0}
    };
    int save_metrics = 0;
    int debug = 0;
    const char *lb_conf = NULL;
    LbModel *lb = NULL;

    int opt;
    while ((opt = getopt_long(argc, argv, "c:sdhm:", long_options, NULL)) != -1) {
        switch (opt) {
            case 'c':
                config_path = optarg;
                break;
            case 's':
                save_metrics = 1;
                break;
            case 'd':
                debug = 1;
                break;
            case 'm': 
                lb_conf = optarg; 
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

    // k8s 인증 정보는 주기 갱신에 계속 쓰므로 프로그램 끝에서 해제
    K8sCreds creds;
    int have_creds = k8s_creds_load(NULL, &creds) == 0;
    if (!have_creds) {
        fprintf(stderr, "k8s 인증 정보 로드 실패, server 목록 비어있음 (갱신 안 함)\n");
    } else if (discover_servers(&creds, &cfg) != 0) {
        fprintf(stderr, "k8s discovery 실패, server 목록 비어있음 (주기 갱신에서 재시도)\n");
        cfg.node_server_count = 0;   // 반쯤 채워진 목록을 쓰지 않게
        cfg.pod_server_count = 0;
    }

    ServerList g_node_server_list;
    server_list_init(&g_node_server_list);
    if (server_list_set(&g_node_server_list, cfg.node_servers, cfg.node_server_count) != 0) {
        fprintf(stderr, "에러: 서버 목록 설정 실패 (서버 개수 초과 가능성)\n");
        return 1;
    }
    
    ServerList g_pod_server_list;
    server_list_init(&g_pod_server_list);
    if (server_list_set(&g_pod_server_list, cfg.pod_servers, cfg.pod_server_count) != 0) {
        fprintf(stderr, "에러: 서버 목록 설정 실패 (서버 개수 초과 가능성)\n");
        return 1;
    }
    
    if (lb_conf) {
    LbModelConfig cfg;
    if (lb_model_config_load(lb_conf, &cfg) != 0 || !(lb = lb_model_create(&cfg)))
        return 1;
    }
    
    AppCollectorConfig app_cfg = {
        .node_server_list = &g_node_server_list,
        .pod_server_list = &g_pod_server_list,
        .save_metrics = save_metrics,
        .lb_model = lb,
        .debug = debug,
    };


    Collector *collector = app_collector_create(&app_cfg);
    if (!collector || collector->init(collector) != 0) {
        fprintf(stderr, "에러: collector 초기화 실패\n");
        //server_source->close(server_source);
        return 1;
    }

    if (lb && lb_model_start(lb) != 0){
        return 1;
    }

     MetricPoster *poster = NULL;
    if (cfg.dashboard_url[0]) {
        MetricPosterConfig pc = { .debug = debug };
        snprintf(pc.url, sizeof(pc.url), "%s", cfg.dashboard_url);
        poster = metric_poster_create(&pc, app_collector_read, collector);
        if (!poster || metric_poster_start(poster) != 0)
            fprintf(stderr, "metric_poster 시작 실패, 대시보드 전송 없이 계속\n");
    } else {
        printf("dashboard_url 없음, 대시보드 전송 안 함\n");
    }

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    // 수집·대시보드 전송·LB 계산은 각자 스레드(gRPC, poster, lb_model)가 하고, main은 서버 목록 갱신만 한다
    int refresh_sec = cfg.refresh_interval_sec > 0 ? cfg.refresh_interval_sec : 3;
    time_t next_refresh = time(NULL) + refresh_sec;   // 처음 목록은 이미 넣었으니 한 주기 뒤부터

    while (running) {
        time_t now = time(NULL);

        // 서버 목록 갱신: k8s 다시 조회 → 목록 교체 → 스트림 맞추기
        if (have_creds && now >= next_refresh) {
            next_refresh = now + refresh_sec;
            if (discover_servers(&creds, &cfg) == 0) {    // 실패하면 기존 목록·스트림 그대로 유지
                server_list_set(&g_node_server_list, cfg.node_servers, cfg.node_server_count);
                server_list_set(&g_pod_server_list,  cfg.pod_servers,  cfg.pod_server_count);
                app_collector_sync(collector);
            }
        }

        sleep(1);
    }

    metric_poster_destroy(poster);
    
    if (lb){
        lb_model_stop(lb);
    }
    
    collector->destroy(collector);
    if (have_creds) k8s_creds_free(&creds);   // ← 추가
    return 0;
    //server_source->close(server_source);
    // mlp_sink->close(mlp_sink);
    return 0;
}
