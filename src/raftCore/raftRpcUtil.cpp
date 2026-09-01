//raft节点间通信的 rpc封装
#include "raftRpcUtil.h"

#include <mprpcchannel.h>
#include <mprpccontroller.h>
#include "config.h"

bool RaftRpcUtil::AppendEntries(raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *response) {
  std::unique_lock<std::mutex> lock(callMutex_, std::try_to_lock);
  if (!lock.owns_lock()) return false;
  MprpcController controller;
  stub_->AppendEntries(&controller, args, response, nullptr);
  return !controller.Failed();
}

bool RaftRpcUtil::InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest *args,
                                  raftRpcProctoc::InstallSnapshotResponse *response) {
  std::unique_lock<std::mutex> lock(callMutex_, std::try_to_lock);
  if (!lock.owns_lock()) return false;
  MprpcController controller;
  stub_->InstallSnapshot(&controller, args, response, nullptr);
  return !controller.Failed();
}

bool RaftRpcUtil::RequestVote(raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *response) {
  std::unique_lock<std::mutex> lock(callMutex_, std::try_to_lock);
  if (!lock.owns_lock()) return false;
  MprpcController controller;
  stub_->RequestVote(&controller, args, response, nullptr);
  return !controller.Failed();
}

//先开启服务器，再尝试连接其他的节点，中间给一个间隔时间，等待其他的rpc服务器节点启动

RaftRpcUtil::RaftRpcUtil(std::string ip, std::uint16_t port) {
  //*********************************************  */
  //发送rpc设置
  stub_ = new raftRpcProctoc::raftRpc_Stub(
      new MprpcChannel(ip, port, false, std::chrono::milliseconds(RaftRpcTimeout)));
}

RaftRpcUtil::~RaftRpcUtil() { delete stub_; }
