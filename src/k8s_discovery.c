#include "k8s_discovery.h"
 
#include <curl/curl.h>
#include <cjson/cJSON.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
 
#define METRICS_PORT 50051
#define POD_METRICS_PORT 50051
#define NODE_METRICS_PORT 50051
 
typedef struct {
    char *data;
    size_t size;
} HttpResponse;
 
static size_t write_cb(void *ptr, size_t size, size_t nmemb, void *userdata) {
    HttpResponse *resp = (HttpResponse *)userdata;
    size_t total = size * nmemb;
    char *tmp = realloc(resp->data, resp->size + total + 1);
    if (!tmp) return 0;
    resp->data = tmp;
    memcpy(resp->data + resp->size, ptr, total);
    resp->size += total;
    resp->data[resp->size] = '\0';
    return total;
}
 
// creds에 담긴 메모리상의 인증서로 mTLS 호출 (디스크에 파일로 안 씀)
static char *k8s_api_get(const K8sCreds *creds, const char *path) {
    char url[512];
    snprintf(url, sizeof(url), "%s%s", creds->server, path);
 
    CURL *curl = curl_easy_init();
    if (!curl) return NULL;
 
    HttpResponse resp = {0};
 
    struct curl_blob ca_blob   = { .data = creds->ca,   .len = creds->ca_len,   .flags = CURL_BLOB_COPY };
    struct curl_blob cert_blob = { .data = creds->cert, .len = creds->cert_len, .flags = CURL_BLOB_COPY };
    struct curl_blob key_blob  = { .data = creds->key,  .len = creds->key_len,  .flags = CURL_BLOB_COPY };
 
    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_CAINFO_BLOB, &ca_blob);
    curl_easy_setopt(curl, CURLOPT_SSLCERT_BLOB, &cert_blob);
    curl_easy_setopt(curl, CURLOPT_SSLCERTTYPE, "PEM");
    curl_easy_setopt(curl, CURLOPT_SSLKEY_BLOB, &key_blob);
    curl_easy_setopt(curl, CURLOPT_SSLKEYTYPE, "PEM");
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);
 
    CURLcode res = curl_easy_perform(curl);
 
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(curl);
 
    if (res != CURLE_OK) {
        fprintf(stderr, "k8s API 호출 실패: %s\n", curl_easy_strerror(res));
        free(resp.data);
        return NULL;
    }
    if (http_code != 200) {
        fprintf(stderr, "k8s API 응답 오류: HTTP %ld, body: %s\n", http_code, resp.data ? resp.data : "");
        free(resp.data);
        return NULL;
    }
    return resp.data;
}

typedef struct {
    char name[64];
    char ip[16];
} NodeNameIp;

// /api/v1/nodes 를 한 번 호출해서 (node 이름, InternalIP) 쌍을 map에 채움.
// 성공 시 0, 실패 시 -1.
static int fetch_node_map(const K8sCreds *creds, NodeNameIp *map, size_t max, size_t *out_count) {
    char *json_str = k8s_api_get(creds, "/api/v1/nodes");
    if (!json_str) return -1;

    cJSON *root = cJSON_Parse(json_str);
    free(json_str);
    if (!root) {
        fprintf(stderr, "node 목록 JSON 파싱 실패\n");
        return -1;
    }

    size_t count = 0;
    cJSON *items = cJSON_GetObjectItem(root, "items");
    cJSON *node;
    cJSON_ArrayForEach(node, items) {
        if (count >= max) {
            fprintf(stderr, "fetch_node_map: 최대 node 개수(%zu) 초과, 나머지 무시됨\n", max);
            break;
        }

        cJSON *metadata = cJSON_GetObjectItem(node, "metadata");
        cJSON *name = metadata ? cJSON_GetObjectItem(metadata, "name") : NULL;
        if (!name || !cJSON_IsString(name)) continue;

        cJSON *status = cJSON_GetObjectItem(node, "status");
        cJSON *addresses = status ? cJSON_GetObjectItem(status, "addresses") : NULL;
        cJSON *addr;
        cJSON_ArrayForEach(addr, addresses) {
            cJSON *type = cJSON_GetObjectItem(addr, "type");
            if (type && cJSON_IsString(type) && strcmp(type->valuestring, "InternalIP") == 0) {
                cJSON *address = cJSON_GetObjectItem(addr, "address");
                if (address && cJSON_IsString(address)) {
                    strncpy(map[count].name, name->valuestring, sizeof(map[count].name) - 1);
                    map[count].name[sizeof(map[count].name) - 1] = '\0';
                    strncpy(map[count].ip, address->valuestring, sizeof(map[count].ip) - 1);
                    map[count].ip[sizeof(map[count].ip) - 1] = '\0';
                    count++;
                }
                break;
            }
        }
    }

    cJSON_Delete(root);
    *out_count = count;
    return 0;
}

// 메모리에 이미 캐싱된 map에서 node 이름으로 IP 조회 (API 호출 없음)
static const char *lookup_node_ip(const NodeNameIp *map, size_t count, const char *node_name) {
    for (size_t i = 0; i < count; i++) {
        if (strcmp(map[i].name, node_name) == 0) {
            return map[i].ip;
        }
    }
    return NULL;
}

// pod의 status.conditions에서 type=Ready 인 항목이 status="True" 인지 확인
// HAProxy(headless DNS)도 Ready인 pod만 쓰므로 같은 기준으로 맞춘다
static int pod_is_ready(cJSON *status) {
    cJSON *conds = status ? cJSON_GetObjectItem(status, "conditions") : NULL;
    cJSON *c;
    cJSON_ArrayForEach(c, conds) {
        cJSON *type = cJSON_GetObjectItem(c, "type");
        cJSON *st   = cJSON_GetObjectItem(c, "status");
        if (cJSON_IsString(type) && strcmp(type->valuestring, "Ready") == 0)
            return cJSON_IsString(st) && strcmp(st->valuestring, "True") == 0;
    }
    return 0;   // Ready 항목이 없으면 아직 준비 안 된 것으로 본다
}

int discover_servers(const K8sCreds *creds, Config *out) {
    out->node_server_count = 0;   // ← 추가
    out->pod_server_count = 0;    // ← 추가

    // 1) node 목록 조회 (한 번만)
    NodeNameIp node_map[MAX_SERVERS];
    size_t node_count = 0;
    if (fetch_node_map(creds, node_map, MAX_SERVERS, &node_count) != 0) {
        return -1;
    }

    // edge pod가 스케줄된 node만 monitoring 대상으로 한다 (pod 목록을 본 뒤 3)에서 등록).
    // origin처럼 sysinfo 서버가 없는 node에 계속 접속을 시도하지 않게 하기 위함
    int node_used[MAX_SERVERS] = {0};

    // 2) pod 목록 조회, 같은 node_map으로 node_key 매칭 (API 재호출 없음)
    char *json_str = k8s_api_get(creds, "/api/v1/namespaces/qos/pods");
    if (!json_str) return -1;

    cJSON *root = cJSON_Parse(json_str);
    free(json_str);
    if (!root) {
        fprintf(stderr, "pod 목록 JSON 파싱 실패\n");
        return -1;
    }

    cJSON *items = cJSON_GetObjectItem(root, "items");
    cJSON *pod;
    cJSON_ArrayForEach(pod, items) {
        if (out->pod_server_count >= MAX_SERVERS) {
            fprintf(stderr, "MAX_SERVERS(%d) 초과, 나머지 pod는 무시됨\n", MAX_SERVERS);
            break;
        }

        cJSON *metadata = cJSON_GetObjectItem(pod, "metadata");
        cJSON *name = metadata ? cJSON_GetObjectItem(metadata, "name") : NULL;
        if (!name || !cJSON_IsString(name)) continue;
        if (strncmp(name->valuestring, "mediasoup-edge-", 15) != 0)
            continue;

        cJSON *status = cJSON_GetObjectItem(pod, "status");
        cJSON *spec   = cJSON_GetObjectItem(pod, "spec");
        cJSON *pod_ip = status ? cJSON_GetObjectItem(status, "podIP") : NULL;
        cJSON *node_name = spec ? cJSON_GetObjectItem(spec, "nodeName") : NULL;

        // pod가 아직 Ready가 아니어도 node에 스케줄됐으면 node는 등록한다.
        // (pod 재시작 때마다 node 스트림까지 닫혔다 열리지 않게)
        if (node_name && cJSON_IsString(node_name)) {
            for (size_t i = 0; i < node_count; i++)
                if (strcmp(node_map[i].name, node_name->valuestring) == 0) node_used[i] = 1;
        }

        if (!pod_ip || !cJSON_IsString(pod_ip) || pod_ip->valuestring[0] == '\0') {
            fprintf(stderr, "pod %s 는 아직 IP가 할당되지 않음 (스킵)\n", name->valuestring);
            continue;
        }

        if (!pod_is_ready(status)) {      // ← 추가
            continue;                     // 로그는 안 찍음 (몇 초마다 반복되므로)
        }
        
        if (!node_name || !cJSON_IsString(node_name)) {
            fprintf(stderr, "pod %s 는 아직 node에 스케줄되지 않음 (스킵)\n", name->valuestring);
            continue;
        }

        const char *node_ip = lookup_node_ip(node_map, node_count, node_name->valuestring);
        if (!node_ip) {
            fprintf(stderr, "pod %s 의 node(%s) IP를 찾을 수 없음 (스킵)\n",
                    name->valuestring, node_name->valuestring);
            continue;
        }

        ServerSlot *entry = &out->pod_servers[out->pod_server_count++];
        memset(entry, 0, sizeof(*entry));
        strncpy(entry->ip, pod_ip->valuestring, sizeof(entry->ip) - 1);
        entry->port = POD_METRICS_PORT;
        strncpy(entry->node_key, node_ip, sizeof(entry->node_key) - 1);
        strncpy(entry->name, name->valuestring, sizeof(entry->name) - 1);
        atomic_store(&entry->status, SERVER_STATUS_UP);

        printf("[discover] pod=%s ip=%s -> node=%s node_ip=%s\n",
               name->valuestring, pod_ip->valuestring,
               node_name->valuestring, node_ip);
    }

    cJSON_Delete(root);

    // 3) edge pod가 있는 node 등록
    for (size_t i = 0; i < node_count; i++) {
        if (!node_used[i]) continue;
        if (out->node_server_count >= MAX_SERVERS) {
            fprintf(stderr, "MAX_SERVERS(%d) 초과, 나머지 node는 무시됨\n", MAX_SERVERS);
            break;
        }

        ServerSlot *entry = &out->node_servers[out->node_server_count++];
        memset(entry, 0, sizeof(*entry));
        strncpy(entry->ip, node_map[i].ip, sizeof(entry->ip) - 1);
        entry->port = NODE_METRICS_PORT;
        strncpy(entry->node_key, node_map[i].ip, sizeof(entry->node_key) - 1); // node 자신의 key = 자기 IP
        strncpy(entry->name, node_map[i].name, sizeof(entry->name) - 1);
        atomic_store(&entry->status, SERVER_STATUS_UP);

        printf("[discover] node=%s ip=%s\n", node_map[i].name, node_map[i].ip);
    }
    return 0;
}