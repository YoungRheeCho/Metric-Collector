#ifndef LB_MODEL_H
#define LB_MODEL_H

// on_update가 받은 값으로 1초마다 서버별 표를 만들어 HAProxy n2sl에 넘기는 모델 스레드.
// 계산은 server_qoe/src/lb/lb_core.py와 같다 (lb_replay로 값이 같은지 확인).

#include <stdint.h>

#include "n2sl_table.h"

#define LB_OBJECTIVE_SERVER 0   // 비용 = 경고 확률 × 포화 시 throttle 초
#define LB_OBJECTIVE_PEOPLE 1   // 위 비용 × (V + k)
#define LB_OBJECTIVE_SIMPLE 2   // 모델 없이 규칙: 여유 서버는 예상 CPU 이용률, 모두 포화면 예상 throttle

typedef struct LbModel LbModel;
typedef struct LbPod LbPod;

typedef struct {
    char model_dir[256];     // warn.txt, crit.txt, sev.txt, meta.json이 있는 폴더 (simple이 아니면 필수)
    char table_shm[64];      // shm_open 이름, 기본 /n2sl_table
    char assign_shm[64];     // shm_open 이름, 기본 /n2sl_assign
    char log_path[256];      // 비어 있으면 로그 없음
    double p_th;             // 자리를 계산할 경고 확률 기준, 기본 0.10
    int objective;           // LB_OBJECTIVE_*, 기본 server
    int num_threads;         // LightGBM 예측 스레드 수, 기본 2
    double sat_pct;          // simple: 이 CPU 이용률(한도 대비 %)부터 포화, 기본 90
    double d_default;        // simple: 시청자 1명당 이용률(%p)을 계산할 수 없을 때 쓰는 값, 기본 1
} LbModelConfig;

typedef struct {
    double V, u, u_peak, dV10, past10, past30, cpu_limit;
    double util1, thr1, thr3;  // simple: 마지막 1초 CPU 이용률(%), 마지막 1초·최근 3초 평균 throttle (초/초)
    double d;                  // simple: 시청자 1명당 이용률(%p) = util1 / V (V가 0이면 다른 서버 평균 또는 d_default)
} LbFeatures;

typedef struct {
    int n;                                        // pod 수
    int64_t t;                                    // 계산한 초
    int ok[N2SL_MAX_SRV];                         // 입력을 만들 수 있었으면 1
    LbFeatures f[N2SL_MAX_SRV];
    double pending[N2SL_MAX_SRV];                 // 보냈지만 V에 아직 안 잡힌 인원
    int32_t seats[N2SL_MAX_SRV];
    float p_warn0[N2SL_MAX_SRV];
    float p_crit0[N2SL_MAX_SRV];
    float cost[N2SL_MAX_SRV][N2SL_KMAX + 2];
} LbResult;

// key=value 파일 (model_dir, table_shm, assign_shm, log_path, p_th, objective=server|people|simple, num_threads,
//                 sat_pct, d_default)
int lb_model_config_load(const char *path, LbModelConfig *out);

// 모델 파일을 읽는다. 실패하면 NULL (이유는 stderr)
LbModel *lb_model_create(const LbModelConfig *cfg);

// 해당 pod의 스트림을 시작하기 전에 호출한다. lb_model_start 전후 언제든 된다 (k8s 목록 갱신으로 생긴 pod).
// key는 HAProxy가 아는 서버 주소(pod IP)와 같아야 한다.
// 같은 key의 빈 칸이 있으면 그 칸을, 없으면 새 칸을, 칸이 다 찼으면 빈 칸 하나를 다시 쓴다. 자리가 없으면 NULL
LbPod *lb_model_add_pod(LbModel *m, const char *key);

// pod가 사라졌을 때, 그 pod의 스트림을 멈춘 뒤에(더 이상 lb_model_add_sample이 안 불릴 때) 호출한다.
// 칸은 key를 남긴 채 "입력 없음"(자리 0, 비용 최대)으로 표에 남는다.
// HAProxy가 아직 그 IP를 들고 있는 몇 초 동안 "표에 없는 서버"가 되어 round robin으로 빠지지 않게 하기 위함
void lb_model_remove_pod(LbModel *m, LbPod *p);

// on_update에서 pod 샘플마다 호출. 값이 없으면 NAN
void lb_model_add_sample(LbPod *p, int64_t recv_ms, double cpu_util_pct, double thr_usec,
                         uint32_t viewer_count, double cpu_limit_cores);

// 공유메모리를 열고 1초 주기 스레드를 시작한다
int lb_model_start(LbModel *m);
void lb_model_stop(LbModel *m);       // 스레드 종료 + join
void lb_model_destroy(LbModel *m);

// 스레드 없이 t초의 표를 계산한다 (점검용). 공유메모리를 열기 전이면 누적 배정 수는 0으로 본다
const LbResult *lb_model_compute(LbModel *m, int64_t t);

#endif
