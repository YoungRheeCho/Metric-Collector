#include "metric.h"

Metric metric_create_app_feature(int out_res, int out_bitrate, int in_res, int in_bitrate){
    Metric m;
    m.type = METRIC_TYPE_APP_FEATURE;
    m.data.app_feature.output_resolution = out_res;
    m.data.app_feature.output_bitrate = out_bitrate;
    m.data.app_feature.input_resolution = in_res;
    m.data.app_feature.input_bitrate = in_bitrate;
    return m;
}

Metric metric_create_system(int node_id, double cpu, double mem, const char* gpu_model, double gpu_util){
    Metric m;
    m.type = METRIC_TYPE_SYSTEM;
    m.data.system.node_id = node_id;
    m.data.system.cpu_util = cpu;
    m.data.system.memory_util = mem;
    strcpy(m.data.system.gpu_model, gpu_model);
    m.data.system.gpu_util = gpu_util;
    return m;
}

Metric metric_create_app_perf(int session_id, int pid, double avg_latency, double avg_fps){
    Metric m;
    m.type = METRIC_TYPE_APP_PERF;
    m.data.app_perf.session_id = session_id;
    m.data.app_perf.pid = pid;
    m.data.app_perf.avg_latency = avg_latency;
    m.data.app_perf.avg_fps = avg_fps;
    return m;
}