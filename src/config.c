#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void trim(char *s) {
    size_t len = strlen(s);
    while (len > 0 &&
           (s[len - 1] == '\n' || s[len - 1] == '\r' || s[len - 1] == ' ' || s[len - 1] == '\t')) {
        s[--len] = '\0';
    }
    size_t start = 0;
    while (s[start] == ' ' || s[start] == '\t')
        start++;
    if (start > 0)
        memmove(s, s + start, len - start + 1);
}

int config_load(const char *path, Config *out) {
    memset(out, 0, sizeof(*out));

    /* 기본값 */
    strncpy(out->metrics_path, "/metrics", sizeof(out->metrics_path) - 1);
    out->worker_pool_size = 1;
    out->collect_interval_sec = 5;
    //out->mlp_ring_capacity = 256;

    FILE *fp = fopen(path, "r");
    if (!fp) {
        perror("config_load: fopen");
        return -1;
    }

    char line[256];
    while (fgets(line, sizeof(line), fp)) {
        trim(line);
        if (line[0] == '\0' || line[0] == '#')
            continue;

        char *eq = strchr(line, '=');
        if (!eq) {
            continue;
        }
        *eq = '\0';

        char *key = line;
        char *value = eq + 1;
        trim(key);
        trim(value);

        if (strcmp(key, "haproxy_shm_name") == 0) {
            strncpy(out->haproxy_shm_name, value, sizeof(out->haproxy_shm_name) - 1);
        } else if (strcmp(key, "metrics_path") == 0) {
            strncpy(out->metrics_path, value, sizeof(out->metrics_path) - 1);
        } else if (strcmp(key, "worker_pool_size") == 0) {
            out->worker_pool_size = atoi(value);
        } else if (strcmp(key, "collect_interval_sec") == 0) {
            out->collect_interval_sec = atoi(value);
        } else if (strcmp(key, "mlp_shm_name") == 0) {
            strncpy(out->mlp_shm_name, value, sizeof(out->mlp_shm_name) - 1);
        } else if(strcmp(key, "refresh_interval_sec") == 0){
            out->refresh_interval_sec = atoi(value);
        }/*else if (strcmp(key, "mlp_ring_capacity") == 0) {
            out->mlp_ring_capacity = (size_t)atol(value);
        }*/
    }

    fclose(fp);

    if (out->haproxy_shm_name[0] == '\0' || out->mlp_shm_name[0] == '\0') {
        fprintf(stderr, "config_load: haproxy_shm_name, mlp_shm_name은 필수입니다\n");
        return -1;
    }

    return 0;
}
