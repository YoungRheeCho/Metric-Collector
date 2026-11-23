# syntax=docker/dockerfile:1

# ============================================================
# Stage 1: Build
# ============================================================
FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# 빌드에 필요한 패키지
#  - build-essential: gcc/g++/make
#  - pkg-config: Makefile의 pkg-config --cflags/--libs grpc++ protobuf 용
#  - protobuf-compiler / protobuf-compiler-grpc: protoc, grpc_cpp_plugin
#  - libgrpc++-dev, libprotobuf-dev: 헤더 + .so + .pc 파일 (pkg-config가 찾는 것들)
#  - python3-pip, unzip: LightGBM wheel을 받아 lib_lightgbm.so를 꺼내는 용도
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    protobuf-compiler \
    protobuf-compiler-grpc \
    libgrpc++-dev \
    libprotobuf-dev \
    libcurl4-openssl-dev \
    libcjson-dev \
    python3-pip \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# 모델 학습에 쓴 것과 같은 버전의 LightGBM 라이브러리
RUN pip3 download lightgbm==4.7.0 --no-deps --only-binary=:all: -d /tmp/lgbm \
    && unzip -j /tmp/lgbm/lightgbm-*.whl 'lightgbm/lib/lib_lightgbm.so' -d /usr/local/lib \
    && ldconfig \
    && rm -rf /tmp/lgbm

WORKDIR /build

# 소스 전체 복사 (Makefile, src/, include/, proto/ 등)
COPY . .

# 실제 빌드 — 기존 Makefile 그대로 사용
RUN make clean && make

# ============================================================
# Stage 2: Runtime (이미지 크기 최소화용 분리 스테이지)
# ============================================================
FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive
ENV HOME=/root

# 런타임에는 -dev 헤더/정적 라이브러리 없이, 공유 라이브러리(.so)만 있으면 됨
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgrpc++1.51t64 \
    libprotobuf32t64 \
    libcurl4t64 \
    libcjson1 \
    libgomp1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 빌드 스테이지에서 바이너리와 LightGBM 라이브러리만 가져옴
COPY --from=builder /build/metric-collector /app/metric-collector
COPY --from=builder /usr/local/lib/lib_lightgbm.so /usr/local/lib/lib_lightgbm.so
RUN ldconfig

# config 파일은 이미지에 포함하지 않음 — 반드시 -v로 마운트해서 사용
#   예: -v $(pwd)/my_collector.conf:/app/sample_collector.conf:ro

# CSV 로그 저장용 디렉토리 (볼륨 마운트 대상)
RUN mkdir -p /app/data

ENTRYPOINT ["/app/metric-collector"]
CMD ["-c", "/app/sample_collector.conf"]