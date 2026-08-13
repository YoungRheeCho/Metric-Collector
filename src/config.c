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
    out->server_count = 0;
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
        } else if (strcmp(key, "server") == 0) {  
            if (out->server_count >= MAX_SERVERS) {
                fprintf(stderr, "config_load: 서버 개수가 MAX_CONFIG_SERVERS(%d)를 초과함\n",
                        MAX_SERVERS);
                continue;
            }
            char *colon = strchr(value, ':');
            if (!colon) {
                fprintf(stderr, "config_load: 잘못된 server 형식 (ip:port 필요): %s\n", value);
                continue;
            }
            *colon = '\0';
            char *ip_str = value;
            char *port_str = colon + 1;

            ServerSlot *entry = &out->servers[out->server_count];
            strncpy(entry->ip, ip_str, sizeof(entry->ip) - 1);
            entry->ip[sizeof(entry->ip) - 1] = '\0';
            entry->port = atoi(port_str);
            atomic_store(&entry->status, SERVER_STATUS_UP);
            out->server_count++;
        }/*else if (strcmp(key, "mlp_ring_capacity") == 0) {
            out->mlp_ring_capacity = (size_t)atol(value);
        }*/
    }

    fclose(fp);

    /*if (out->haproxy_shm_name[0] == '\0' || out->mlp_shm_name[0] == '\0') {
        fprintf(stderr, "config_load: haproxy_shm_name, mlp_shm_name은 필수입니다\n");
        return -1;
    }*/

    return 0;
}
