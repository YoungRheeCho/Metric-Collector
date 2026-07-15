#ifndef CONFIG_H
#define CONFIG_H

#include <stddef.h>

typedef struct {
    char haproxy_shm_name[64];
    char metrics_path[128];
    int worker_pool_size;
    int collect_interval_sec;
    int refresh_interval_sec;
    char mlp_shm_name[64];
    //size_t mlp_ring_capacity;
} Config;

int config_load(const char *path, Config *out);

#endif
