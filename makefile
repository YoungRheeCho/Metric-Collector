CC       := gcc
CXX      := g++
CFLAGS   := -Wall -Wextra -Wswitch -std=gnu11 -Iinclude -MMD -MP
CXXFLAGS := -Wall -Wextra -std=c++17 -Iinclude -Iproto_gen -MMD -MP \
            $(shell pkg-config --cflags grpc++ protobuf)
LDFLAGS  := -lpthread -lrt $(shell pkg-config --libs grpc++ protobuf) -ldl

SRC_DIR    := src
PROTO_DIR  := proto
GEN_DIR    := proto_gen
BUILD_DIR  := build
TARGET     := metric-collector

PROTOC          := protoc
GRPC_CPP_PLUGIN := $(shell which grpc_cpp_plugin)

# .proto → 생성될 파일 목록
PROTO_SRC   := $(PROTO_DIR)/sysinfo.proto
PROTO_GEN_SRCS := $(GEN_DIR)/sysinfo.pb.cc $(GEN_DIR)/sysinfo.grpc.pb.cc

# 일반 C 소스
SRCS_C := $(shell find $(SRC_DIR) -name '*.c')
OBJS_C := $(patsubst $(SRC_DIR)/%.c, $(BUILD_DIR)/%.o, $(SRCS_C))

# C++ 소스 (wrapper.cpp 등)
SRCS_CXX := $(shell find $(SRC_DIR) -name '*.cpp')
OBJS_CXX := $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(SRCS_CXX))

# proto 생성 소스 → 오브젝트
OBJS_PROTO := $(patsubst $(GEN_DIR)/%.cc, $(BUILD_DIR)/%.o, $(PROTO_GEN_SRCS))

OBJS := $(OBJS_C) $(OBJS_CXX) $(OBJS_PROTO)
DEPS := $(OBJS:.o=.d)

.PHONY: all clean rebuild

all: $(TARGET)

# 1) proto 코드 생성 (sysinfo.proto가 바뀌면 다시 생성됨)
$(PROTO_GEN_SRCS): $(PROTO_SRC)
	@mkdir -p $(GEN_DIR)
	$(PROTOC) -I $(PROTO_DIR) --cpp_out=$(GEN_DIR) --grpc_out=$(GEN_DIR) \
	    --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN) $(PROTO_SRC)

# 2) 최종 링크는 반드시 g++ (C++ 오브젝트가 섞이므로)
$(TARGET): $(OBJS)
	$(CXX) $(OBJS) -o $@ $(LDFLAGS)

# 3) 일반 .c 컴파일 (gcc)
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

# 4) .cpp 컴파일 (g++) — wrapper.cpp 등
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp $(PROTO_GEN_SRCS)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# 5) proto가 생성한 .cc 컴파일 (g++), proto_gen 도 먼저 생성돼야 함
$(BUILD_DIR)/%.o: $(GEN_DIR)/%.cc $(PROTO_GEN_SRCS)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -rf $(BUILD_DIR) $(GEN_DIR) $(TARGET)

rebuild: clean all

-include $(DEPS)