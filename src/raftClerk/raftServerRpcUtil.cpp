//客户端和kvserver通信的rpc封装
// 这是 Clerk 调用 KVServer 的 RPC 客户端封装，只包含 caller 逻辑。
// callee 逻辑在 KvServer 里实现，并通过 RpcProvider::NotifyService(this) 注册。
// Raft 节点之间的 RPC 使用另一套 RaftRpcUtil。
#include "raftServerRpcUtil.h"
raftServerRpcUtil::raftServerRpcUtil(std::string ip, short port) {
 
  stub = new raftKVRpcProctoc::kvServerRpc_Stub(new MprpcChannel(ip, port, false));
}

raftServerRpcUtil::~raftServerRpcUtil() { delete stub; }

bool raftServerRpcUtil::Get(raftKVRpcProctoc::GetArgs *GetArgs, raftKVRpcProctoc::GetReply *reply) {
  MprpcController controller;
  stub->Get(&controller, GetArgs, reply, nullptr);
  return !controller.Failed();
}

bool raftServerRpcUtil::PutAppend(raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply) {
  MprpcController controller;
  stub->PutAppend(&controller, args, reply, nullptr);
  if (controller.Failed()) {
    std::cout << controller.ErrorText() << endl;
  }
  return !controller.Failed();
}

bool raftServerRpcUtil::GetStatus(raftKVRpcProctoc::StatusReply *reply) {
  raftKVRpcProctoc::StatusArgs args;
  MprpcController controller;
  stub->GetStatus(&controller, &args, reply, nullptr);
  return !controller.Failed();
}
