#ifndef SERVER_LIST_H
#define SERVER_LIST_H

#include "channel.h"
#include "shared_types.h"
#include <pthread.h>
#include <stddef.h>
#include <signal.h>

#define MAX_SERVERS 128

typedef struct {
    ServerSlot servers[MAX_SERVERS];
    size_t count;
    pthread_mutex_t mutex;
} ServerList;

typedef struct {
    Channel *channel; // 어디서 읽어올지 (HAProxy shared memory 채널)
    ServerList *list; // 어디에 갱신해 넣을지 (main.c가 만든 g_server_list)
    int interval_sec; // 몇 초마다 갱신할지
    volatile sig_atomic_t *running;
} RefresherArgs;

int server_list_init(ServerList *list);
int server_list_snapshot(ServerList *list, ServerSlot *out, size_t max, size_t *out_count);
void server_list_destroy(ServerList *list);
void* refresher_main(void* arg);

#endif