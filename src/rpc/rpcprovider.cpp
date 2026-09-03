#include "rpcprovider.h"
#include <arpa/inet.h>
#include <cstring>
#include <string>
#include "rpcheader.pb.h"
#include "rpc_frame.h"
#include "util.h"

namespace {

class OneShotClosure final : public google::protobuf::Closure {
 public:
  explicit OneShotClosure(std::function<void()> callback) : callback_(std::move(callback)) {}

  void Run() override {
    auto callback = std::move(callback_);
    delete this;
    callback();
  }

 private:
  std::function<void()> callback_;
};

}  // namespace
/*
service_name =>  service描述
                        =》 service* 记录服务对象
                        method_name  =>  method方法对象
m_serviceMap["kvServerRpc"]
  m_methodMap["PutAppend"]
  m_methodMap["Get"]
*/

// todo 待修改 要把本机开启的ip和端口写在文件里面
void RpcProvider::NotifyService(google::protobuf::Service *service) {
  ServiceInfo service_info;
  const google::protobuf::ServiceDescriptor *pserviceDesc = service->GetDescriptor();// 获取了服务对象的描述信息
  std::string service_name = pserviceDesc->name();// 获取服务的名字
  int methodCnt = pserviceDesc->method_count();// 获取服务对象service的方法的数量
  std::cout << "service_name:" << service_name << std::endl;

  for (int i = 0; i < methodCnt; ++i) {
    // 获取了服务对象指定下标的服务方法的描述（抽象描述） UserService   Login
    const google::protobuf::MethodDescriptor *pmethodDesc = pserviceDesc->method(i);
    std::string method_name = pmethodDesc->name();
    service_info.m_methodMap.insert({method_name, pmethodDesc});
  }
  service_info.m_service = service;
  m_serviceMap.insert({service_name, service_info});
}


// 启动rpc服务节点，开始提供rpc远程网络调用服务
void RpcProvider::Run(const std::string& bindIp, std::uint16_t port) {
  //创建服务器
  muduo::net::InetAddress address(bindIp, port);

  // 创建TcpServer对象
  m_muduo_server = std::make_shared<muduo::net::TcpServer>(&m_eventLoop, address, "RpcProvider");

  // 绑定连接回调和消息读写回调方法  分离了网络代码和业务代码
  m_muduo_server->setConnectionCallback(std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
  m_muduo_server->setMessageCallback(std::bind(&RpcProvider::OnMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
  
  // 设置muduo库的线程数量
  m_muduo_server->setThreadNum(4);

  // rpc服务端准备启动，打印信息
  std::cout << "RpcProvider start service at ip:" << bindIp << " port:" << port << std::endl;

  // 启动网络服务
  m_muduo_server->start();// 启动 TcpServer，开始监听端口并启动内部 IO 线程。
  m_eventLoop.loop();// 进入事件循环，当前线程会阻塞在这里，持续等待并处理网络事件。当有新连接、数据到达或连接断开时，muduo 会触发前面注册的回调函数。
 
}

// 新socket连接回调
void RpcProvider::OnConnection(const muduo::net::TcpConnectionPtr &conn) {
  // 如果是新连接就什么都不干，即正常的接收连接即可
  if (!conn->connected()) {
    conn->shutdown(); // 和rpc client的连接断开
  }
}

/* 
RpcHeader：service_name method_name args_size   UserService  Login   zhang san123456                         
data：header_size(4个字节) + header_str + args_str
*/
// 已建立连接用户的读写事件回调：解析请求，根据服务名，方法名，参数，来调用service的来callmethod来调用本地的业务
void RpcProvider::OnMessage(const muduo::net::TcpConnectionPtr &conn, muduo::net::Buffer *buffer, muduo::Timestamp) {
  while (true) {
    if (buffer->readableBytes() >= sizeof(std::uint32_t)) {
      std::uint32_t networkLength = 0;
      std::memcpy(&networkLength, buffer->peek(), sizeof(networkLength));
      if (ntohl(networkLength) > mprpc::kMaxRpcFrameSize) {
        std::cout << "rpc frame exceeds maximum size" << std::endl;
        conn->forceClose();
        return;
      }
    }

    std::string recvBuf;
    if (!mprpc::TryConsumeRpcFrame(buffer, &recvBuf)) return;

    google::protobuf::io::ArrayInputStream arrayInput(recvBuf.data(), recvBuf.size());
    google::protobuf::io::CodedInputStream codedInput(&arrayInput);
    uint32_t headerSize{};
    if (!codedInput.ReadVarint32(&headerSize)) {
      std::cout << "rpc request header length parse error" << std::endl;
      conn->forceClose();
      return;
    }

    std::string rpcHeaderStr;
    const auto headerLimit = codedInput.PushLimit(headerSize);
    if (!codedInput.ReadString(&rpcHeaderStr, headerSize)) {
      std::cout << "rpc header read error" << std::endl;
      conn->forceClose();
      return;
    }
    codedInput.PopLimit(headerLimit);

    RPC::RpcHeader rpcHeader;
    if (!rpcHeader.ParseFromString(rpcHeaderStr)) {
      std::cout << "rpc header parse error" << std::endl;
      conn->forceClose();
      return;
    }

    std::string argsStr;
    if (!codedInput.ReadString(&argsStr, rpcHeader.args_size())) {
      std::cout << "rpc request arguments read error" << std::endl;
      conn->forceClose();
      return;
    }
    if (codedInput.CurrentPosition() != static_cast<int>(recvBuf.size())) {
      std::cout << "rpc request has trailing data" << std::endl;
      conn->forceClose();
      return;
    }

    auto serviceIt = m_serviceMap.find(rpcHeader.service_name());
    if (serviceIt == m_serviceMap.end()) {
      std::cout << "service " << rpcHeader.service_name() << " does not exist" << std::endl;
      conn->forceClose();
      return;
    }
    auto methodIt = serviceIt->second.m_methodMap.find(rpcHeader.method_name());
    if (methodIt == serviceIt->second.m_methodMap.end()) {
      std::cout << "method " << rpcHeader.method_name() << " does not exist" << std::endl;
      conn->forceClose();
      return;
    }

    google::protobuf::Service *service = serviceIt->second.m_service;
    const google::protobuf::MethodDescriptor *method = methodIt->second;
    google::protobuf::Message *request = service->GetRequestPrototype(method).New();
    if (!request->ParseFromString(argsStr)) {
      std::cout << "rpc request parse error" << std::endl;
      conn->forceClose();
      delete request;
      return;
    }
    google::protobuf::Message *response = service->GetResponsePrototype(method).New();
    google::protobuf::Closure *done = new OneShotClosure([this, conn, request, response] {
      SendRpcResponse(conn, response);
      delete response;
      delete request;
    });
    service->CallMethod(method, nullptr, request, response, done);
  }
}

// Closure的回调操作，用于序列化rpc的响应和网络发送,发送响应回去
void RpcProvider::SendRpcResponse(const muduo::net::TcpConnectionPtr &conn, google::protobuf::Message *response) {
  std::string response_str;
  if (response->SerializeToString(&response_str))  // response进行序列化
  {
    if (response_str.size() > mprpc::kMaxRpcFrameSize) {
      std::cout << "rpc response exceeds maximum size" << std::endl;
      conn->forceClose();
    } else {
      conn->send(mprpc::EncodeRpcFrame(response_str));// 序列化成功后，通过网络把rpc方法执行的结果发送会rpc的调用方
    }
  } else {
    std::cout << "serialize response_str error!" << std::endl;
  }
}

RpcProvider::~RpcProvider() {
  if (m_muduo_server) {
    std::cout << "[func - RpcProvider::~RpcProvider()]: ip和port信息：" << m_muduo_server->ipPort() << std::endl;
  }
  m_eventLoop.quit();
  
}
