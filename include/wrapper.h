#ifndef WRAPPER_H
#define WRAPPER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {            // proto NetCounters (누적)
    uint64_t tx_bytes;
    uint64_t tx_packets;
    uint64_t tx_drop;
} SysNet;

typedef struct {            // proto Mem (현재값)
    double used_mb;
    double total_mb;        // container에서 무제한이면 -1
} SysMem;

typedef struct {
    bool has_cpu;
    int64_t cpu_idle;       // /proc/stat 누적 jiffies
    int64_t cpu_total;

    bool has_mem;
    SysMem mem;

    bool has_net;
    SysNet net;             // 물리 NIC

    bool has_qdisc;
    char qdisc_kind[16];    // "mq", "fq_codel" ...
    uint64_t qdisc_bytes;   // 누적
    uint32_t qdisc_packets; // 누적 (32비트)
    uint32_t qdisc_qlen;    // 현재값
    uint32_t qdisc_backlog; // 현재값
    uint32_t qdisc_drops;   // 누적 (32비트)
    uint32_t qdisc_requeues;
    uint32_t qdisc_overlimits;
} NodeInfo;

typedef struct {
    bool has_cpu;
    uint64_t cpu_usage_usec;      // 누적
    double cpu_limit_cores;       // 무제한이면 -1
    uint64_t cpu_nr_periods;      // 누적
    uint64_t cpu_nr_throttled;    // 누적
    uint64_t cpu_throttled_usec;  // 누적

    bool has_mem;
    SysMem mem;

    bool has_net;
    SysNet net;                   // pod 네트워크

    uint32_t viewer_count;
} ContainerInfo;

typedef enum {
    SYSINFO_KIND_NONE = 0,        // node도 container도 아님 (비정상 응답)
    SYSINFO_KIND_NODE,
    SYSINFO_KIND_CONTAINER
} SysInfoKind;

typedef struct {
    SysInfoKind kind;             // 아래 union 중 어느 쪽이 유효한지
    int64_t recv_ms;              // wrapper가 응답을 받은 시각 (unix ms) — Δt 계산용
    union {
        NodeInfo node;
        ContainerInfo container;
    };
} SystemInfo;

// 서버가 push할 때마다 이 콜백이 한 번씩 호출된다.
// user_data는 stream_system_info() 호출 시 넘긴 값이 그대로 전달된다.
typedef void (*SystemInfoCallback)(const SystemInfo* info, void* user_data);

// 스트림을 열고 블로킹하며 계속 값을 받는다.
// 서버 연결이 끊기거나 에러가 나면 리턴한다 (성공적으로 끝나면 0, 실패하면 -1).
// server_address 예: "server:50051" 또는 "localhost:50051"
void* start_stream(const char* server_address, SystemInfoCallback callback, void* user_data);

// start_stream()이 리턴한 핸들을 정리(연결 종료 + 메모리 해제)한다.
void stop_stream(void* handle);
// 스트림이 끝났으면(연결 실패 포함) 1
int stream_is_done(void* handle);
// 이 스트림으로 값을 한 번이라도 받았으면 1 (재연결 backoff 리셋 판단용)
int stream_received_any(void* handle);
// 끝난 스트림의 에러 메시지를 buf에 복사 (stream_is_done이 1일 때 의미 있음)
void stream_last_error(void* handle, char* buf, size_t len);

#ifdef __cplusplus
}
#endif

#endif