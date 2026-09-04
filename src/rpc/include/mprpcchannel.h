#ifndef MPRPCCHANNEL_H
#define MPRPCCHANNEL_H
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstddef>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <random>  // 包含 std::uniform_int_distribution 类型的头文件
#include <string>
#include <unordered_map>
#include <vector>
using namespace std;
//RPC 客户端真正负责“打包请求、发网络、收响应”的通道类
// 真正负责发送和接受的前后处理工作，如消息的组织方式，向哪个节点发送等等
class MprpcChannel : public google::protobuf::RpcChannel {
 public:
  // 所有通过stub代理对象调用的rpc方法，都走到这里了，统一做rpc方法调用的数据数据序列化和网络发送 那一步
  void CallMethod(const google::protobuf::MethodDescriptor *method, google::protobuf::RpcController *controller,
                  const google::protobuf::Message *request, google::protobuf::Message *response,
                  google::protobuf::Closure *done) override;
  MprpcChannel(std::string ip, std::uint16_t port, bool connectNow,
               std::chrono::milliseconds ioTimeout = std::chrono::milliseconds(300));
  ~MprpcChannel() override;

 private:
  int m_clientFd;
  const std::string m_ip;  //保存ip和端口，如果断了可以尝试重连
  const uint16_t m_port;
  const std::chrono::milliseconds m_ioTimeout;
  std::mutex m_callMutex;
  bool SendAll(const char* data, std::size_t size);
  bool RecvExact(char* data, std::size_t size);
  bool newConnect(const char *ip, uint16_t port, string *errMsg);
};

#endif  // MPRPCCHANNEL_H
