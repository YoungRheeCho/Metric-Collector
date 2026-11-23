#ifndef K8S_CREDS_H
#define K8S_CREDS_H
 
#include <stddef.h>
 
// kubeconfig에서 읽어온 자격증명 (메모리에만 보관, 디스크에 안 씀)
typedef struct {
    char server[256];       // API server 주소 (예: https://<api-server-ip>:6443)
    unsigned char *ca;       // 디코딩된 CA 인증서 (PEM)
    size_t ca_len;
    unsigned char *cert;     // 디코딩된 client 인증서
    size_t cert_len;
    unsigned char *key;      // 디코딩된 client 개인키
    size_t key_len;
} K8sCreds;
 
// kubeconfig 파일(path)을 읽어서 server/CA/cert/key를 파싱 및 base64 디코딩해 creds에 채움.
// path가 NULL이면 $KUBECONFIG, 그것도 없으면 $HOME/.kube/config 를 사용.
// 성공 시 0, 실패 시 -1.
int k8s_creds_load(const char *path, K8sCreds *creds);
 
// creds가 들고 있는 메모리를 해제. 개인키는 free 전에 0으로 덮어씀.
void k8s_creds_free(K8sCreds *creds);
 
#endif