CC      := gcc
CFLAGS  := -Wall -Wextra -Wswitch -std=gnu11 -Iinclude -MMD -MP
LDFLAGS := -lpthread -lrt

SRC_DIR   := src
BUILD_DIR := build
TARGET    := metric-collector

# src/ 아래 모든 .c 파일 재귀적으로 찾기 (kubernetes/, mlp/ 등 하위 폴더 포함)
SRCS := $(shell find $(SRC_DIR) -name '*.c')
OBJS := $(patsubst $(SRC_DIR)/%.c, $(BUILD_DIR)/%.o, $(SRCS))
DEPS := $(OBJS:.o=.d)

.PHONY: all clean rebuild

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(OBJS) -o $@ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf $(BUILD_DIR) $(TARGET)

# clean 하고 바로 이어서 처음부터 다시 빌드 (make rebuild)
rebuild: clean all

-include $(DEPS)


#설명

#- -Iinclude: 지난번 얘기한 대로, 어떤 하위 폴더의 .c든 #include "metric.h"처럼 상대 경로 없이 쓸 수 있게 해줘.
#- -MMD -MP: 컴파일할 때 .d 파일(의존성 목록)을 자동 생성해서, 헤더 파일 하나만 고쳐도 그걸 include하는 .c 파일들이 자동으로 다시 컴파일돼. 이거 없으면 헤더만 고쳤을 때 make가 눈치 못 채고 재빌드를 안 해줘서 오래된 바이너리로 테스트하는 실수가 생길 수 있어.
#- find $(SRC_DIR) -name '*.c': 재귀적으로 소스 찾기. kubernetes/, mlp/ 폴더가 생겨도 자동으로 포함돼.
# - $(BUILD_DIR)/%.o: $(SRC_DIR)/%.c: 패턴 규칙 — src/kubernetes/k8s_client.c → build/kubernetes/k8s_client.o처럼 폴더 구조를 그대로 유지하면서 오브젝트를 만들어줘.

# 나중에 추가로 고려할 것

# - tests/ 디렉토리가 실제로 채워지면 test 타겟을 별도로 추가하는 것도 고려.
# - Debug/Release 구분하고 싶으면 CFLAGS에 -g -O0(디버그) / -O2(릴리즈)를 조건부로 넣는 방법도 있어.

# 지금 규모(파일 몇 개)에서는 이 정도면 충분하고, 프로젝트 커지면 그때 확장하면 될 것 같아.
