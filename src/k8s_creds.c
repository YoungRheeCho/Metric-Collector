#include "k8s_creds.h"
 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
 
/* ================= base64 디코더 (표준 base64, 의존성 없이 직접 구현) ================= */
static int b64_val(unsigned char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1; // '=' 나 개행 등 무시 대상
}
 
static unsigned char *base64_decode(const char *in, size_t in_len, size_t *out_len) {
    unsigned char *out = malloc(in_len);
    if (!out) return NULL;
 
    size_t oi = 0;
    int vals[4];
    int vc = 0;
 
    for (size_t i = 0; i < in_len; i++) {
        unsigned char c = (unsigned char)in[i];
        if (c == '=' || c == '\n' || c == '\r' || c == ' ') continue;
        int v = b64_val(c);
        if (v < 0) continue;
        vals[vc++] = v;
        if (vc == 4) {
            out[oi++] = (vals[0] << 2) | (vals[1] >> 4);
            out[oi++] = ((vals[1] & 0xF) << 4) | (vals[2] >> 2);
            out[oi++] = ((vals[2] & 0x3) << 6) | vals[3];
            vc = 0;
        }
    }
    if (vc == 2) {
        out[oi++] = (vals[0] << 2) | (vals[1] >> 4);
    } else if (vc == 3) {
        out[oi++] = (vals[0] << 2) | (vals[1] >> 4);
        out[oi++] = ((vals[1] & 0xF) << 4) | (vals[2] >> 2);
    }
 
    *out_len = oi;
    return out;
}
 
/* ================= kubeconfig 라인 파싱 ================= */
static const char *match_field(const char *line, const char *key) {
    const char *p = line;
    while (*p == ' ' || *p == '\t') p++;
    size_t klen = strlen(key);
    if (strncmp(p, key, klen) != 0) return NULL;
    p += klen;
    if (*p != ':') return NULL;
    p++;
    while (*p == ' ' || *p == '\t') p++;
    return p;
}
 
static void decode_field_into(const char *value, unsigned char **out_buf, size_t *out_len) {
    size_t vlen = strlen(value);
    while (vlen > 0 && (value[vlen-1] == '\n' || value[vlen-1] == '\r')) vlen--;
    *out_buf = base64_decode(value, vlen, out_len);
}
 
int k8s_creds_load(const char *path, K8sCreds *out) {
    memset(out, 0, sizeof(*out));
 
    char resolved_path[512];
    if (!path) {
        const char *env_kubeconfig = getenv("KUBECONFIG");
        if (env_kubeconfig) {
            strncpy(resolved_path, env_kubeconfig, sizeof(resolved_path) - 1);
        } else {
            const char *home = getenv("HOME");
            if (!home) {
                fprintf(stderr, "k8s_creds_load: HOME 환경변수를 찾을 수 없음\n");
                return -1;
            }
            snprintf(resolved_path, sizeof(resolved_path), "%s/.kube/config", home);
        }
        path = resolved_path;
    }
 
    FILE *fp = fopen(path, "r");
    if (!fp) {
        fprintf(stderr, "k8s_creds_load: kubeconfig 열기 실패: %s\n", path);
        return -1;
    }
 
    char line[8192]; // base64 인증서 한 줄이 길 수 있어 넉넉하게
    int got_server = 0, got_ca = 0, got_cert = 0, got_key = 0;
 
    while (fgets(line, sizeof(line), fp)) {
        const char *v;
 
        // single-cluster/single-user kubeconfig 전제: 첫 번째로 발견되는 값을 사용
        if (!got_server && (v = match_field(line, "server"))) {
            strncpy(out->server, v, sizeof(out->server) - 1);
            size_t l = strlen(out->server);
            while (l > 0 && (out->server[l-1] == '\n' || out->server[l-1] == '\r')) out->server[--l] = '\0';
            got_server = 1;
            continue;
        }
        if (!got_ca && (v = match_field(line, "certificate-authority-data"))) {
            decode_field_into(v, &out->ca, &out->ca_len);
            got_ca = out->ca != NULL;
            continue;
        }
        if (!got_cert && (v = match_field(line, "client-certificate-data"))) {
            decode_field_into(v, &out->cert, &out->cert_len);
            got_cert = out->cert != NULL;
            continue;
        }
        if (!got_key && (v = match_field(line, "client-key-data"))) {
            decode_field_into(v, &out->key, &out->key_len);
            got_key = out->key != NULL;
            continue;
        }
    }
    fclose(fp);
 
    if (!got_server || !got_ca || !got_cert || !got_key) {
        fprintf(stderr, "k8s_creds_load: kubeconfig 파싱 실패 (server=%d ca=%d cert=%d key=%d)\n",
                got_server, got_ca, got_cert, got_key);
        k8s_creds_free(out);
        return -1;
    }
 
    return 0;
}
 
void k8s_creds_free(K8sCreds *c) {
    free(c->ca);
    free(c->cert);
    if (c->key) {
        memset(c->key, 0, c->key_len); // 개인키는 메모리에서도 바로 지움
        free(c->key);
    }
    memset(c, 0, sizeof(*c));
}