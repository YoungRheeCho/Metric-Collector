#include "config.h"

#include <getopt.h>
#include <stdio.h>
#include <string.h>

/*
 * config_load()가 config 파일을 제대로 파싱하는지 눈으로 확인하기 위한
 * 간단한 실행 프로그램. 정식 유닛 테스트 프레임워크는 아니고,
 * 값을 읽어와서 출력 + 필수 필드 검증 정도만 함.
 *
 * 사용법: ./test_config -c <config 파일 경로>
 *         (main.c와 동일하게 -c/--config로 받음, 인자 없으면 에러)
 */

static void print_config(const Config* cfg) {
    printf("haproxy_shm_name   = \"%s\"\n", cfg->haproxy_shm_name);
    printf("metrics_path       = \"%s\"\n", cfg->metrics_path);
    printf("worker_pool_size   = %d\n", cfg->worker_pool_size);
    printf("refresh_interval_sec = %d\n", cfg->refresh_interval_sec);
    printf("mlp_shm_name       = \"%s\"\n", cfg->mlp_shm_name);
}

int main(int argc, char* argv[]) {
    const char* path = NULL;

    static struct option long_options[] = {
        {"config", required_argument, NULL, 'c'},
        {"help",   no_argument,       NULL, 'h'},
        {NULL, 0, NULL, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "c:h", long_options, NULL)) != -1) {
        switch (opt) {
            case 'c':
                path = optarg;
                break;
            case 'h':
                printf("사용법: %s -c <config file>\n", argv[0]);
                return 0;
            default:
                fprintf(stderr, "사용법: %s -c <config file>\n", argv[0]);
                return 1;
        }
    }

    if (path == NULL) {
        fprintf(stderr, "에러: config 파일 경로가 필요합니다 (-c 또는 --config)\n");
        return 1;
    }

    printf("[test_config] loading: %s\n", path);

    Config cfg;
    if (config_load(path, &cfg) != 0) {
        fprintf(stderr, "[test_config] FAIL: config_load()가 실패를 리턴함\n");
        return 1;
    }

    print_config(&cfg);

    int ok = 1;

    if (strcmp(cfg.haproxy_shm_name, "/haproxy_server_list") != 0) {
        fprintf(stderr, "[test_config] FAIL: haproxy_shm_name 불일치\n");
        ok = 0;
    }
    if (strcmp(cfg.metrics_path, "/metrics") != 0) {
        fprintf(stderr, "[test_config] FAIL: metrics_path 불일치\n");
        ok = 0;
    }
    if (cfg.worker_pool_size != 4) {
        fprintf(stderr, "[test_config] FAIL: worker_pool_size 불일치 (got %d)\n", cfg.worker_pool_size);
        ok = 0;
    }
    if (ok) {
        printf("[test_config] PASS\n");
        return 0;
    }
    return 1;
}
