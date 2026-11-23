#ifndef K8S_DISCOVERY_H
#define K8S_DISCOVERY_H
 
#include "config.h"
#include "k8s_creds.h"
 
// qos namespace의 mediasoup-edge-* pod들을 조회해서
// out->servers 에 pod IP(:50051) + 소속 node IP(node_key)를 채워넣음.
// out은 미리 memset(0)으로 초기화되어 있어야 함 (server_count가 0부터 시작).
// 성공 시 0, 실패 시 -1.
int discover_servers(const K8sCreds *creds, Config *out);
 
#endif