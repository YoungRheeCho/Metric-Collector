# Metric-Collector

## 📌 Overview

**Metric-Collector**는 시스템 및 클러스터의 상태 지표(metric)를 수집하여, MLP(의사결정 프로세스)가 판단을 내리는 데 필요한 데이터를 제공하는 프로세스입니다.

전체 시스템은 아래와 같이 3개의 프로세스로 구성되며, Metric-Collector는 그 중 **데이터 수집을 담당하는 역할**을 맡습니다.

```
┌────────────────────┐      metrics      ┌──────────────────┐
│  Metric-Collector   │ ───────────────▶  │    MLP Process    │
│  (본 레포지토리)      │                    │  (의사결정 프로세스) │
└─────────┬──────────┘                    └──────────────────┘
          │
          │ collects metrics from
          ▼
┌────────────────────┐
│     Kubernetes      │
│     (클러스터 상태)   │
└────────────────────┘
```

## 🎯 역할 (Role)

- 시스템 리소스(CPU, Memory, Network 등) 및 Kubernetes 클러스터 상태 지표 수집
- 수집한 metric을 가공하여 MLP 프로세스가 사용할 수 있는 형태로 전달
- 향후 다양한 metric source(예: Prometheus 등) 확장을 고려한 모듈형 구조 지향

## 📁 Directory Structure

```
Metric-Collector/
├── include/                  # 헤더 파일 (공개 인터페이스, 구조체 정의)
│   ├── metric.h               # Metric 구조체 및 공통 타입 정의
│   ├── collector.h            # Collector 공통 인터페이스 (collect, flush 등)
│   ├── kubernetes/
│   │   └── k8s_client.h       # Kubernetes 연동 인터페이스
│   └── mlp/
│       └── mlp_client.h       # MLP 프로세스 연동 인터페이스
│
├── src/                       # 실제 구현 코드
│   ├── main.c                  # 프로그램 진입점
│   ├── metric.c                 # Metric 구조체 관련 로직 (생성/해제/변환 등)
│   ├── kubernetes/
│   │   └── k8s_client.c        # Kubernetes API 연동 구현
│   └── mlp/
│       └── mlp_client.c        # MLP 프로세스와의 통신 구현
│
├── tests/                     # 단위 테스트 (예정)
│   ├── test_metric.c
│   └── test_k8s_client.c
│
├── config/                    # 설정 파일 (예정)
│   └── collector.conf
│
├── Makefile
├── README.md
└── .gitignore
```

### 폴더별 설명

| 폴더/파일 | 설명 |
|---|---|
| `include/metric.h` | 수집되는 metric의 공통 구조체 및 타입 정의 |
| `include/collector.h` | 여러 metric source(k8s, mlp 등)가 공통으로 따르는 인터페이스 정의 |
| `include/kubernetes/`, `src/kubernetes/` | Kubernetes 클러스터로부터 상태 정보를 가져오는 로직 |
| `include/mlp/`, `src/mlp/` | 수집한 metric을 MLP 프로세스로 전달하는 로직 |
| `src/main.c` | 전체 흐름 제어 (수집 → 가공 → 전달) |
| `tests/` | 각 모듈에 대한 단위 테스트 |
| `config/` | 수집 주기, 대상 등 설정 값 관리 |

## 🛠 Build & Run

```bash
make
./metric-collector
```

## 🔗 관련 프로세스

- **MLP Process**: Metric-Collector가 수집한 데이터를 기반으로 실제 의사결정을 수행하는 프로세스
- **Kubernetes**: Metric-Collector가 상태 정보를 수집하는 대상 클러스터

## 📋 브랜치 컨벤션

| Prefix | 용도 |
|---|---|
| `feature/` | 새로운 기능 추가 |
| `fix/` | 버그 수정 |
| `refactor/` | 리팩토링 |
| `docs/` | 문서/주석 관련 |

## ✅ TODO

- [ ] Kubernetes 연동 구현
- [ ] MLP 통신 프로토콜 정의
- [ ] 설정 파일(config) 로딩 기능
- [ ] 단위 테스트 작성
