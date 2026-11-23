#ifndef APP_COLLECTOR_H
#define APP_COLLECTOR_H

#include "server_list.h"
#include "collector.h"
#include "lb_model.h"
#include "metric_poster.h"

typedef struct {
    ServerList *node_server_list;
    ServerList *pod_server_list;
    //char metrics_path[128];
    //int worker_pool_size;
    LbModel *lb_model;
    int debug;
    int save_metrics;
} AppCollectorConfig;

Collector *app_collector_create(const AppCollectorConfig *config);
// metric_poster의 read 콜백. ctx는 app_collector_create가 돌려준 Collector*
int app_collector_read(void *ctx, MetricPosterNode *nodes, size_t max_nodes, size_t *nn,
                       MetricPosterPod *pods, size_t max_pods, size_t *np);

void app_collector_sync(Collector *self);

#endif