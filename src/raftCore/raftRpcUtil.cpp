//raft节点间通信的 rpc封装
#include "raftRpcUtil.h"

#include <mprpcchannel.h>
#include <mprpccontroller.h>
#include <chrono>
#include <thread>
#include "config.h"

namespace {

template <typename Reply, typename Invoke>
bool InvokeWithFault(std::mutex& mutex, const std::filesystem::path& policyPath, int source, int target,
                     RaftRpcMethod method, Reply* response, Invoke&& invoke) {
  std::unique_lock<std::mutex> lock(mutex, std::try_to_lock);
  if (!lock.owns_lock()) return false;
  const auto decision = ReadRaftFaultPolicy(policyPath, source, target, method);
  if (decision.action == RaftFaultAction::Drop) return false;
  if (decision.action == RaftFaultAction::Delay && decision.delay.count() > 0) {
    std::this_thread::sleep_for(decision.delay);
  }
  const bool firstOk = invoke(response);
  if (decision.action != RaftFaultAction::Duplicate) return firstOk;
  Reply duplicateResponse;
  (void)invoke(&duplicateResponse);
  return firstOk;
}

}  // namespace

bool RaftRpcUtil::AppendEntries(raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *response) {
  return InvokeWithFault<raftRpcProctoc::AppendEntriesReply>(callMutex_, faultPolicy_, sourceId_, targetId_,
      RaftRpcMethod::AppendEntries, response, [&](raftRpcProctoc::AppendEntriesReply* result) {
        MprpcController controller;
        stub_->AppendEntries(&controller, args, result, nullptr);
        return !controller.Failed();
      });
}

bool RaftRpcUtil::InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest *args,
                                  raftRpcProctoc::InstallSnapshotResponse *response) {
  return InvokeWithFault<raftRpcProctoc::InstallSnapshotResponse>(callMutex_, faultPolicy_, sourceId_, targetId_,
      RaftRpcMethod::InstallSnapshot, response, [&](raftRpcProctoc::InstallSnapshotResponse* result) {
        MprpcController controller;
        stub_->InstallSnapshot(&controller, args, result, nullptr);
        return !controller.Failed();
      });
}

bool RaftRpcUtil::RequestVote(raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *response) {
  return InvokeWithFault<raftRpcProctoc::RequestVoteReply>(callMutex_, faultPolicy_, sourceId_, targetId_,
      RaftRpcMethod::RequestVote, response, [&](raftRpcProctoc::RequestVoteReply* result) {
        MprpcController controller;
        stub_->RequestVote(&controller, args, result, nullptr);
        return !controller.Failed();
      });
}

//先开启服务器，再尝试连接其他的节点，中间给一个间隔时间，等待其他的rpc服务器节点启动

RaftRpcUtil::RaftRpcUtil(std::string ip, std::uint16_t port)
    : RaftRpcUtil(std::move(ip), port, -1, -1, {}) {}

RaftRpcUtil::RaftRpcUtil(std::string ip, std::uint16_t port, int sourceId, int targetId,
                         std::filesystem::path faultPolicy)
    : sourceId_(sourceId), targetId_(targetId), faultPolicy_(std::move(faultPolicy)) {
  //*********************************************  */
  //发送rpc设置
  stub_ = new raftRpcProctoc::raftRpc_Stub(
      new MprpcChannel(ip, port, false, std::chrono::milliseconds(RaftRpcTimeout)));
}

RaftRpcUtil::~RaftRpcUtil() { delete stub_; }
