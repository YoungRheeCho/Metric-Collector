#ifndef APP_COLLECTOR_H
#define APP_COLLECTOR_H

#include "server_list.h"
#include "collector.h"

typedef struct {
    ServerList *node_server_list;
    ServerList *pod_server_list;
    //char metrics_path[128];
    //int worker_pool_size;
    int debug;
    int save_metrics;
} AppCollectorConfig;

Collector *app_collector_create(const AppCollectorConfig *config);

#endif