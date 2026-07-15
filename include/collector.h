#ifndef COLLECTOR_H
#define COLLECTOR_H

#include "metric.h"
#include <stdio.h>

typedef struct Collector Collector;

struct Collector {
    const char *collector_id; // collector 디버깅 용
    int (*init)(Collector *self);
    int (*collect)(Collector *self, Metric *out, size_t max_count, size_t *out_count);
    int (*flush)(Collector *self);
    void (*destroy)(Collector *self);
    void *impl_data; // 구현체별 내부 상태 (k8s 클라이언트 핸들 등)
};

#endif