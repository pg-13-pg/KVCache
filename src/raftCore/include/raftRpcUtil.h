//raft集群通信
//raft集群中每个节点都要维护一个rpc连接，即MprpcChannel，来和其他节点进行通信，发送请求和接收响应

#ifndef RAFTRPC_H
#define RAFTRPC_H

#include <cstdint>
#include <mutex>
#include "raftRPC.pb.h"

/// @brief 维护当前节点对其他某一个结点的所有rpc发送通信的功能
// 对于一个raft节点来说，对于任意其他的节点都要维护一个rpc连接，即MprpcChannel
class RaftRpcUtil {
 private:
  raftRpcProctoc::raftRpc_Stub *stub_;    // raftRpcProctoc
  std::mutex callMutex_;

 public:
  //主动调用其他节点的三个方法,可以按照mit6824来调用，但是别的节点调用自己的好像就不行了，要继承protoc提供的service类才行
  bool AppendEntries(raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *response);
  bool InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest *args, raftRpcProctoc::InstallSnapshotResponse *response);
  bool RequestVote(raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *response);
  //响应其他节点的方法
  /**
   *
   * @param ip  远端ip
   * @param port  远端端口
   */
  RaftRpcUtil(std::string ip, std::uint16_t port);
  ~RaftRpcUtil();
};

#endif  // RAFTRPC_H


// node1 本地:
// RaftRpcUtil::InstallSnapshot(...)
//     |
//     v
// stub_->InstallSnapshot(&controller, args, response, nullptr)
//     |
//     v
// RPC 框架通过 MprpcChannel 发送网络请求到 node2  src/rpc/...
//     |
//     v
// node2 本地:
// Raft::InstallSnapshot(controller, request, response, done)
//     |
//     v
// node2 本地真正处理:
// Raft::InstallSnapshot(request, response)
