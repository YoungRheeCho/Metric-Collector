// 구조체 타입
#include <stdint.h>

typedef enum {
    METRIC_TYPE_APP_FEATURE = 0,
    METRIC_TYPE_SYSTEM = 1,
    METRIC_TYPE_APP_PERF = 2
} MetricType;

// App Feature
typedef struct {
    int output_resolution;
    int output_bitrate;
    int input_resolution;
    int input_bitrate;
} AppFeatureMetric;

// System Metric
typedef struct {
    int    node_id;
    double cpu_util;
    double memory_util;
    char  gpu_model[32];
    double gpu_util;
} SystemMetric;

// App performance
typedef struct {
    int    session_id;
    int    pid;
    double avg_latency;
    double avg_fps;
} AppPerfMetric;


// MLP와의 통신에 사용할 구조체
typedef struct {
    int32_t type;
    union {
        AppFeatureMetric app_feature;
        SystemMetric     system;
        AppPerfMetric    app_perf;
    } data;
} Metric;

Metric metric_create_app_feature(int out_res, int out_bitrate, int in_res, int in_bitrate);
Metric metric_create_system(int node_id, double cpu, double mem, const char* gpu_model, double gpu_util);
Metric metric_create_app_perf(int session_id, int pid, double avg_latency, double avg_fps);