//客户端Clerk 和 KVServer 通信
#ifndef RAFTSERVERRPC_H
#define RAFTSERVERRPC_H
#include <chrono>
#include <cstdint>
#include <iostream>
#include "kvServerRPC.pb.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "rpcprovider.h"

class raftServerRpcUtil {
 private:
  raftKVRpcProctoc::kvServerRpc_Stub* stub;   //raftKVRpcProctoc

 public:

  //响应其他节点的方法
  bool Get(raftKVRpcProctoc::GetArgs* GetArgs, raftKVRpcProctoc::GetReply* reply);
  bool PutAppend(raftKVRpcProctoc::PutAppendArgs* args, raftKVRpcProctoc::PutAppendReply* reply);
  bool GetStatus(raftKVRpcProctoc::StatusReply* reply);

  raftServerRpcUtil(std::string ip, std::uint16_t port,
                    std::chrono::milliseconds ioTimeout = std::chrono::milliseconds(300));
  ~raftServerRpcUtil();
};

#endif  
