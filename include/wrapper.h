#ifndef WRAPPER_H
#define WRAPPER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>

typedef struct {
    double cpu_util_percent;
    double mem_util_percent;
    double mem_total_mb;
    double mem_used_mb;

    bool has_viewer_count;      // 값이 존재하는지
    uint32_t viewer_count;      // 실제 값 (has_viewer_count가 true일 때만 유효)
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

#ifdef __cplusplus
}
#endif

#endif