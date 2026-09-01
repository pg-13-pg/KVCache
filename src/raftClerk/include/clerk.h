#ifndef SKIP_LIST_ON_RAFT_CLERK_H
#define SKIP_LIST_ON_RAFT_CLERK_H
#include <arpa/inet.h>
#include <netinet/in.h>
#include <raftServerRpcUtil.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <chrono>
#include <string>
#include <vector>
#include "kvServerRPC.pb.h"

enum class ClerkStatus { Ok, NotFound, TimedOut, Unavailable };

class Clerk {
 private:
  std::vector<std::shared_ptr<raftServerRpcUtil>>m_servers; //// 保存所有 KVServer 节点的 RPC 客户端 //todo：全部初始化为-1，表示没有连接上
  std::string m_clientId;
  int m_requestId;
  int m_recentLeaderId;  //记录最近一次成功联系的 Leader
  //用于返回随机的clientId
  std::string Uuid() {
    return std::to_string(rand()) + std::to_string(rand()) + std::to_string(rand()) + std::to_string(rand());
  }  
  //    MakeClerk  todo
  ClerkStatus PutAppendUntil(const std::string& key, const std::string& value,
                             const std::string& op,
                             std::chrono::steady_clock::time_point deadline);

 public:
  //对外暴露的三个功能和初始化
  void Init(std::string configFileName,
            std::chrono::milliseconds ioTimeout = std::chrono::milliseconds(300));
  std::string Get(std::string key);
  void Put(std::string key, std::string value);
  void Append(std::string key, std::string value);
  ClerkStatus GetUntil(const std::string& key, std::string* value,
                       std::chrono::steady_clock::time_point deadline);
  ClerkStatus PutUntil(const std::string& key, const std::string& value,
                       std::chrono::steady_clock::time_point deadline);
  ClerkStatus AppendUntil(const std::string& key, const std::string& value,
                          std::chrono::steady_clock::time_point deadline);

 public:
  Clerk();
};

#endif  // SKIP_LIST_ON_RAFT_CLERK_H
