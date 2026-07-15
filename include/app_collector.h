#ifndef APP_COLLECTOR_H
#define APP_COLLECTOR_H

#include "server_list.h"
#include "collector.h"

typedef struct {
    ServerList *server_list;
    char metrics_path[128];
    int worker_pool_size;
} AppCollectorConfig;

Collector *app_collector_create(const AppCollectorConfig *config);

#endif