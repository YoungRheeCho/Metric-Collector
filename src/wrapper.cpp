#include "../include/wrapper.h"
#include "sysinfo.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <memory>
#include <cstdio>

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::Status;
using sysinfo::Empty;
using sysinfo::SysInfoService;
using sysinfo::SystemInfoResponse;


class StreamReactor : public grpc::ClientReadReactor<SystemInfoResponse> {
private:
    std::shared_ptr<Channel> channel_;
    std::unique_ptr<SysInfoService::Stub> stub_;
    Empty request_;
    SystemInfoResponse response_;
    ClientContext context_;
    SystemInfoCallback callback_;
    void* user_data_;

public:
    StreamReactor(const char* server_address, SystemInfoCallback cb, void* user_data)
        : callback_(cb), user_data_(user_data) {
        channel_ = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
        stub_ = SysInfoService::NewStub(channel_);
        stub_->async()->StreamSystemInfo(&context_, &request_, this);
        StartRead(&response_);
        StartCall();
    }

    void OnReadDone(bool ok) override {
        if (!ok) return;   // 스트림 끊김 — 재연결은 바깥에서 처리

        SystemInfo info;
        info.cpu_util_percent = response_.cpu_util_percent();
        info.mem_util_percent = response_.mem_util_percent();
        info.mem_total_mb = response_.mem_total_mb();
        info.mem_used_mb = response_.mem_used_mb();
        callback_(&info, user_data_);

        StartRead(&response_);
    }

    void OnDone(const grpc::Status& s) override {
        if (!s.ok()) {
            fprintf(stderr, "gRPC 스트림 종료: %s\n", s.error_message().c_str());
        }
        // 필요하면 여기서 재연결 트리거
    }

};

extern "C" void* start_stream(const char* server_address, SystemInfoCallback callback, void* user_data) {
    if (server_address == nullptr || callback == nullptr) return nullptr;
    return new StreamReactor(server_address, callback, user_data);   // 즉시 리턴, 블로킹 없음
}

extern "C" void stop_stream(void* handle) {
    delete static_cast<StreamReactor*>(handle);
}