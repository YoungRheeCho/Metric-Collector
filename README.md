# Metric-Collector

## 🎯 역할 (Role)
 
- 각 edge node/pod에서 동작하는 metric agent(gRPC 서버)에 **지속 연결(스트림)** 을 맺고, 서버가 push하는 값을 실시간으로 수신
- 서버(=pod 또는 node)별 최신 metric을 스레드 안전하게 캐싱
- 주기적으로(`collect_interval_sec`) 캐시된 값을 모아 MLP 프로세스로 전달 *(현재 `main.c`에 인터페이스만 있고 실제 전송 로직은 TODO)*
- `-s`(`--save-metrics`) 옵션 시, 서버별로 CSV 파일에 timestamp/CPU/메모리/시청자수(있는 경우)를 기록 — MLP 모델 학습용 데이터셋 수집 용도
- `-d`(`--debug`) 옵션 시, 수신되는 metric을 표준출력에 실시간 로그로 출력

## 🛠 Build & Run
 
### 요구 사항
- gRPC / Protobuf 개발 패키지 (`grpc++`, `protobuf`) — `pkg-config`로 조회 가능해야 함
- `protoc`, `grpc_cpp_plugin`
### Build
```bash
make            # proto 코드 생성 → C/C++ 오브젝트 빌드 → 링크
make clean      # build/, proto_gen/, 바이너리 삭제
make rebuild    # clean + all
```
 
### Run
```bash
./metric-collector -c <config file> [-s|--save-metrics] [-d|--debug]
```
 
| 옵션 | 설명 |
|---|---|
| `-c, --config <path>` | 필수. 수집 대상 서버 목록 등을 담은 config 파일 경로 |
| `-s, --save-metrics` | 서버별 수신 metric을 `./data/`(또는 설정된 경로) 아래 CSV로 저장 |
| `-d, --debug` | 수신되는 metric을 표준출력에 실시간 출력 |
| `-h, --help` | 사용법 출력 |
 
### Docker
```bash
docker build -t metric-collector:v1 .
docker run --network host --rm \
  -v $(pwd)/my_collector.conf:/app/sample_collector.conf:ro \
  -v $(pwd)/data:/app/data \
  metric-collector:v1 -s -d
```
클러스터 pod IP(flannel 오버레이 네트워크)로 접속해야 하므로, 컨테이너로 실행 시 `--network host`가 필요합니다 (master node에서 직접 실행할 경우).