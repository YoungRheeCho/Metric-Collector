#include "server_list.h"
#include <stdlib.h> // malloc (RefresherArgs 관련)
#include <string.h> // memcpy
#include <unistd.h> // sleep

int server_list_init(ServerList *list) {
    int r;
    list->count = 0;
    r = pthread_mutex_init(&list->mutex, NULL) == 0 ? 0 : -1;
    return r;
}

int server_list_snapshot(ServerList *list, ServerSlot *out, size_t max, size_t *out_count) {
    pthread_mutex_lock(&list->mutex);
    size_t n = list->count < max ? list->count : max;
    memcpy(out, list->servers, n * sizeof(ServerSlot));
    if (out_count) {
        *out_count = n;
    }
    pthread_mutex_unlock(&list->mutex);
    return 0;
}

void server_list_destroy(ServerList *list) { pthread_mutex_destroy(&list->mutex); }

void *refresher_main(void *arg) {
    RefresherArgs *args = (RefresherArgs *)arg;
    while (*args->running) {
        server_list_refresh(args->list, args->channel);
        sleep((unsigned int)args->interval_sec);
    }
    return NULL;
}