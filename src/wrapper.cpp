#include "../include/wrapper.h"
#include "sysinfo.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <memory>
#include <cstdio>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <string>

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
    std::mutex mu_;
    std::condition_variable cv_;
    bool done_ = false;
    std::string error_;                    // OnDone의 에러 메시지 (출력은 app_collector가 상태가 바뀔 때만)
    std::atomic<bool> received_{false};    // 한 번이라도 값을 받았는지 (재연결 backoff 리셋 판단용)
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
        received_.store(true, std::memory_order_relaxed);

        SystemInfo info{};   // C++에서는 {0} 대신 {} (enum 필드 때문)
        info.recv_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        if (response_.has_node()) {
            const sysinfo::NodeSample& s = response_.node();
            NodeInfo& n = info.node;
            info.kind = SYSINFO_KIND_NODE;

            n.has_cpu = s.has_cpu();
            if (n.has_cpu) {
                n.cpu_idle = s.cpu().idle();
                n.cpu_total = s.cpu().total();
            }
            n.has_mem = s.has_mem();
            if (n.has_mem) {
                n.mem.used_mb = s.mem().used_mb();
                n.mem.total_mb = s.mem().total_mb();
            }
            n.has_net = s.has_net();
            if (n.has_net) {
                n.net.tx_bytes = s.net().tx_bytes();
                n.net.tx_packets = s.net().tx_packets();
                n.net.tx_drop = s.net().tx_drop();
            }
            n.has_qdisc = s.has_qdisc();
            if (n.has_qdisc) {
                const sysinfo::Qdisc& q = s.qdisc();
                std::snprintf(n.qdisc_kind, sizeof(n.qdisc_kind), "%s", q.kind().c_str());
                n.qdisc_bytes = q.bytes();
                n.qdisc_packets = q.packets();
                n.qdisc_qlen = q.qlen();
                n.qdisc_backlog = q.backlog();
                n.qdisc_drops = q.drops();
                n.qdisc_requeues = q.requeues();
                n.qdisc_overlimits = q.overlimits();
            }
        } else if (response_.has_container()) {
            const sysinfo::ContainerSample& s = response_.container();
            ContainerInfo& c = info.container;
            info.kind = SYSINFO_KIND_CONTAINER;

            c.has_cpu = s.has_cpu();
            if (c.has_cpu) {
                c.cpu_usage_usec = s.cpu().usage_usec();
                c.cpu_limit_cores = s.cpu().limit_cores();
                c.cpu_nr_periods = s.cpu().nr_periods();
                c.cpu_nr_throttled = s.cpu().nr_throttled();
                c.cpu_throttled_usec = s.cpu().throttled_usec();
            }
            c.has_mem = s.has_mem();
            if (c.has_mem) {
                c.mem.used_mb = s.mem().used_mb();
                c.mem.total_mb = s.mem().total_mb();
            }
            c.has_net = s.has_net();
            if (c.has_net) {
                c.net.tx_bytes = s.net().tx_bytes();
                c.net.tx_packets = s.net().tx_packets();
                c.net.tx_drop = s.net().tx_drop();
            }
            c.viewer_count = s.viewer_count();
        }
        // 둘 다 없으면 kind = SYSINFO_KIND_NONE 그대로 (콜백에서 판단)

        callback_(&info, user_data_);

        StartRead(&response_);
    }

    void OnDone(const grpc::Status& s) override {
        // 여기서 출력하지 않는다: 재연결할 때마다 같은 에러가 반복되므로 app_collector가 상태 변화 때만 출력
        std::lock_guard<std::mutex> lk(mu_);
        if (s.ok())
            error_ = "서버가 스트림을 정상 종료함";
        else
            error_ = s.error_message();
        done_ = true;
        cv_.notify_all();
    }

    bool IsDone() { std::lock_guard<std::mutex> lk(mu_); return done_; }
    bool Received() { return received_.load(std::memory_order_relaxed); }
    void LastError(char* buf, size_t len) {
        std::lock_guard<std::mutex> lk(mu_);
        std::snprintf(buf, len, "%s", error_.c_str());
    }
    
    void CancelAndWait() {
            context_.TryCancel();
            std::unique_lock<std::mutex> lk(mu_);
            cv_.wait(lk, [this] { return done_; });
    }
};


extern "C" void* start_stream(const char* server_address, SystemInfoCallback callback, void* user_data) {
    if (server_address == nullptr || callback == nullptr) return nullptr;
    return new StreamReactor(server_address, callback, user_data);   // 즉시 리턴, 블로킹 없음
}

extern "C" void stop_stream(void* h) {
    auto* r = static_cast<StreamReactor*>(h);
    if (!r) return;
    r->CancelAndWait();   // 이후엔 콜백이 더 오지 않음
    delete r;
}

extern "C" int stream_is_done(void* h) {
    return h ? static_cast<StreamReactor*>(h)->IsDone() : 1;
}

extern "C" int stream_received_any(void* h) {
    return h ? static_cast<StreamReactor*>(h)->Received() : 0;
}

extern "C" void stream_last_error(void* h, char* buf, size_t len) {
    if (!buf || len == 0) return;
    if (!h) { std::snprintf(buf, len, "스트림 시작 실패"); return; }
    static_cast<StreamReactor*>(h)->LastError(buf, len);
}