#ifndef METRIC_POSTER_H
#define METRIC_POSTER_H

// 노드·pod 값을 1초마다 origin 대시보드에 POST하는 스레드.
//   POST <url>   예: http://<origin>:18080/api/central/sysinfo
//   형식은 mediasoup-dashboard docs/api.md "Metric-Collector 입력 (origin)"
//
// 보내는 값은 모두 누적 카운터의 "직전 전송(1초 전) 이후 Δ"로 계산한 1초 구간 값이다.
// on_update의 0.1초 Δ(CSV, LB용)와 섞이지 않게 기준값은 이 모듈이 따로 들고 있다.
// 대시보드가 꺼져 있으면 그 초는 버리고 다음 초에 다시 보낸다 (재시도·쌓아두기 없음).
// 실패해도 기준값은 갱신한다 → 복구 직후에도 항상 1초 구간 값이 나간다.

#include <stddef.h>
#include <stdint.h>

#include "wrapper.h"

#define METRIC_POSTER_MAX      128   // 노드·pod 각각 최대 개수 (MAX_SERVERS와 같게)
#define METRIC_POSTER_NAME_LEN 64

typedef struct MetricPoster MetricPoster;

// read 콜백이 채우는 노드 하나
typedef struct {
    char name[METRIC_POSTER_NAME_LEN];   // k8s 노드 이름. 대시보드의 NODE_NAME과 같아야 화면에서 맞춰진다
    int valid;                           // 슬롯에 값이 한 번도 안 들어왔으면 0
    SystemInfo info;                     // 슬롯의 최신 원본값 (kind == SYSINFO_KIND_NODE)
    double cpu_peak_pct;                 // 직전 read 이후 0.1초 구간 CPU 사용률 중 최대 (%). 없으면 NAN
} MetricPosterNode;

// read 콜백이 채우는 pod 하나
typedef struct {
    char name[METRIC_POSTER_NAME_LEN];   // pod 이름. edge 대시보드의 MSDASH_SERVER_ID와 같아야 한다
    char node[METRIC_POSTER_NAME_LEN];   // 소속 노드 이름. 모르면 빈 문자열 (→ JSON null)
    int valid;
    SystemInfo info;                     // kind == SYSINFO_KIND_CONTAINER
    double cpu_peak_pct;                 // 직전 read 이후 0.1초 구간 CPU 사용률(한도 대비) 중 최대 (%). 없으면 NAN
} MetricPosterPod;

// poster 스레드가 1초마다 한 번 부른다. 슬롯 값을 복사해서 채우고 0을 돌려준다 (실패면 -1 → 그 초는 건너뜀).
// 슬롯 mutex는 이 함수 안에서 복사하는 동안만 잡는다.
// cpu_peak_pct는 읽으면서 비운다 (read-and-reset) → 매초 "그 1초 안의 0.1초 값 중 최대"가 된다.
typedef int (*MetricPosterReadFn)(void *ctx,
                                  MetricPosterNode *nodes, size_t max_nodes, size_t *node_count,
                                  MetricPosterPod *pods, size_t max_pods, size_t *pod_count);

typedef struct {
    char url[256];        // 필수
    int timeout_ms;       // 한 번 전송에 쓰는 최대 시간, 0이면 500 (1초보다 짧아야 다음 초를 안 밀어낸다)
    int debug;            // 1이면 매초 보낸 내용을 한 줄씩 출력
} MetricPosterConfig;

// url이 비었거나 메모리가 없으면 NULL
MetricPoster *metric_poster_create(const MetricPosterConfig *cfg, MetricPosterReadFn read, void *ctx);

// 1초 주기 스레드 시작 / 종료(+join). stop은 여러 번 불러도 된다
int metric_poster_start(MetricPoster *p);
void metric_poster_stop(MetricPoster *p);
void metric_poster_destroy(MetricPoster *p);

// 스레드 없이 한 번만: 읽기 → Δ 계산 → POST. 성공 0, 실패 -1 (점검용)
int metric_poster_tick(MetricPoster *p);

// 스레드 없이 한 번만: 읽기 → Δ 계산 → JSON 문자열 (free로 해제). POST하지 않는다. 기준값은 갱신된다 (점검용)
char *metric_poster_build_json(MetricPoster *p);

#endif
