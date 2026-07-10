#pragma once
#include <google/protobuf/descriptor.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/net/TcpServer.h>
#include <functional>
#include <string>
#include <unordered_map>
#include "google/protobuf/service.h"
// todo:现在rpc客户端变成了长连接，因此rpc服务器这边最好提供一个定时器，用以断开很久没有请求的连接。
//客户端也需要配合：每次发送 RPC 前检查连接状态， 如果连接已断开，则重新建立连接后再发送。
class RpcProvider {
 public:

  void NotifyService(google::protobuf::Service *service);// 这里是框架提供给外部使用的，可以发布rpc方法的函数接口
  void Run(int nodeIndex, short port);// 启动rpc服务节点，开始提供rpc远程网络调用服务

 private:
  // 组合EventLoop
  muduo::net::EventLoop m_eventLoop;
  std::shared_ptr<muduo::net::TcpServer> m_muduo_server;
  // service服务详细信息（包含服务对象和方法信息）
  struct ServiceInfo {
    google::protobuf::Service *m_service;                                                     // 保存服务对象
    std::unordered_map<std::string, const google::protobuf::MethodDescriptor *> m_methodMap;  // 保存服务方法
  };
  // service_name =>  service描述
  std::unordered_map<std::string, ServiceInfo> m_serviceMap;
  // 新的socket连接回调
  void OnConnection(const muduo::net::TcpConnectionPtr &);
  // 已建立连接用户的读写事件回调
  void OnMessage(const muduo::net::TcpConnectionPtr &, muduo::net::Buffer *, muduo::Timestamp);
  // Closure的回调操作，用于序列化rpc的响应和网络发送
  void SendRpcResponse(const muduo::net::TcpConnectionPtr &, google::protobuf::Message *);

 public:
  ~RpcProvider();
};