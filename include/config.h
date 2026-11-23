#ifndef CONFIG_H
#define CONFIG_H

#include <stddef.h>
#include "server_list.h"

typedef struct {
    char haproxy_shm_name[64];
    char metrics_path[128];
    int worker_pool_size;
    int refresh_interval_sec;
    char mlp_shm_name[64];
    //size_t mlp_ring_capacity;
    ServerSlot pod_servers[MAX_SERVERS];
    ServerSlot node_servers[MAX_SERVERS];
    size_t pod_server_count;
    size_t node_server_count;
    char dashboard_url[256];
} Config;

int config_load(const char *path, Config *out);

#endif
