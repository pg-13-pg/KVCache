#include "mprpcchannel.h"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include <string>
#include "mprpccontroller.h"
#include "rpcheader.pb.h"
#include "util.h"


// 所有通过stub代理对象调用的rpc方法，都会走到这里了，统一通过rpcChannel来调用方法
// 统一做rpc方法调用的数据序列化和网络发送
void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                              google::protobuf::RpcController* controller, const google::protobuf::Message* request,
                              google::protobuf::Message* response, google::protobuf::Closure* done) {
  //没有连接或者连接已经断开，那么就要重新连接呢,会一直不断地重试
  if (m_clientFd == -1) {
    std::string errMsg;
    bool rt = newConnect(m_ip.c_str(), m_port, &errMsg);
    if (!rt) {
      controller->SetFailed(errMsg);
      return;
    }
  }

  const google::protobuf::ServiceDescriptor* sd = method->service();//method为方法的描述
  std::string service_name = sd->name();     // service_name
  std::string method_name = method->name();  // method_name

  // 获取参数的序列化字符串长度 args_size
  uint32_t args_size{};
  std::string args_str;
  if (request->SerializeToString(&args_str)) {//请求参数序列化
    args_size = args_str.size();
  } else {
    controller->SetFailed("serialize request error!");
    return;
  }
  RPC::RpcHeader rpcHeader;
  rpcHeader.set_service_name(service_name);
  rpcHeader.set_method_name(method_name);
  rpcHeader.set_args_size(args_size);

  std::string rpc_header_str;
  if (!rpcHeader.SerializeToString(&rpc_header_str)) {//请求头序列化
    controller->SetFailed("serialize rpc header error!");
    return;
  }

   //构建 send_rpc_str： header_size + (service_name method_name args_size)RpcHeader + args
  std::string send_rpc_str;  // 用来存储最终发送的数据
  {  //coded_output->string_output->send_rpc_str
    google::protobuf::io::StringOutputStream string_output(&send_rpc_str);
    google::protobuf::io::CodedOutputStream coded_output(&string_output);

    // 先写入header的长度（变长编码）
    coded_output.WriteVarint32(static_cast<uint32_t>(rpc_header_str.size()));

    // 然后写入rpc_header本身
    coded_output.WriteString(rpc_header_str);
  }
  // 最后，将请求参数附加到send_rpc_str后面
   send_rpc_str += args_str;

  std::size_t sent = 0;
  while (sent < send_rpc_str.size()) {
    const auto result = send(m_clientFd, send_rpc_str.data() + sent,
                             send_rpc_str.size() - sent, MSG_NOSIGNAL);
    if (result > 0) {
      sent += static_cast<std::size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) continue;
    const std::string error = "send failed: " + std::string(std::strerror(errno));
    close(m_clientFd);
    m_clientFd = -1;
    controller->SetFailed(error);
    return;
  }
 
  // 接收rpc请求的响应值
  char recv_buf[1024] = {0};
  int recv_size = 0;
  do {
    recv_size = recv(m_clientFd, recv_buf, sizeof(recv_buf), 0);
  } while (recv_size < 0 && errno == EINTR);
  if (recv_size <= 0) {
    const std::string error = recv_size == 0
                                  ? "connection closed before RPC response"
                                  : "receive failed: " + std::string(std::strerror(errno));
    close(m_clientFd);
    m_clientFd = -1;
    controller->SetFailed(error);
    return;
  }

  // 反序列化rpc调用的响应数据存入response
  if (!response->ParseFromArray(recv_buf, recv_size)) {
    close(m_clientFd);
    m_clientFd = -1;
    controller->SetFailed("parse RPC response failed");
    return;
  }
}

//创建一个 TCP socket，并连接到指定的 RPC 服务端 ip:port，连接成功后把 fd 保存到 m_clientFd。
bool MprpcChannel::newConnect(const char* ip, uint16_t port, string* errMsg) {
  int clientfd = socket(AF_INET, SOCK_STREAM, 0);
  if (-1 == clientfd) {
    char errtxt[512] = {0};
    sprintf(errtxt, "create socket error! errno:%d", errno);
    m_clientFd = -1;
    *errMsg = errtxt;
    return false;
  }
  const int originalFlags = fcntl(clientfd, F_GETFL, 0);
  if (originalFlags < 0 || fcntl(clientfd, F_SETFL, originalFlags | O_NONBLOCK) < 0) {
    *errMsg = "configure nonblocking connect failed";
    close(clientfd);
    m_clientFd = -1;
    return false;
  }
  struct sockaddr_in server_addr{};
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  if (inet_pton(AF_INET, ip, &server_addr.sin_addr) != 1) {
    *errMsg = "invalid IPv4 address: " + std::string(ip);
    close(clientfd);
    m_clientFd = -1;
    return false;
  }

  int connectResult = connect(clientfd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr));
  if (connectResult < 0 && errno != EINPROGRESS) {
    *errMsg = "connect failed: " + std::string(std::strerror(errno));
    close(clientfd);
    m_clientFd = -1;
    return false;
  }
  if (connectResult < 0) {
    pollfd descriptor{clientfd, POLLOUT, 0};
    int pollResult;
    do {
      pollResult = poll(&descriptor, 1, static_cast<int>(m_ioTimeout.count()));
    } while (pollResult < 0 && errno == EINTR);
    if (pollResult <= 0) {
      *errMsg = pollResult == 0 ? "connect timed out"
                                : "connect poll failed: " + std::string(std::strerror(errno));
      close(clientfd);
      m_clientFd = -1;
      return false;
    }
    int socketError = 0;
    socklen_t errorSize = sizeof(socketError);
    if (getsockopt(clientfd, SOL_SOCKET, SO_ERROR, &socketError, &errorSize) < 0 || socketError != 0) {
      if (socketError == 0) socketError = errno;
      *errMsg = "connect failed: " + std::string(std::strerror(socketError));
      close(clientfd);
      m_clientFd = -1;
      return false;
    }
  }
  if (fcntl(clientfd, F_SETFL, originalFlags) < 0) {
    *errMsg = "restore socket flags failed";
    close(clientfd);
    m_clientFd = -1;
    return false;
  }
  timeval timeout{};
  timeout.tv_sec = static_cast<time_t>(m_ioTimeout.count() / 1000);
  timeout.tv_usec = static_cast<suseconds_t>((m_ioTimeout.count() % 1000) * 1000);
  if (setsockopt(clientfd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0 ||
      setsockopt(clientfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
    *errMsg = "configure socket timeout failed";
    close(clientfd);
    m_clientFd = -1;
    return false;
  }
  m_clientFd = clientfd;
  return true;
}

//创建一个 RPC 客户端通道，保存目标服务端的 ip/port，并根据 connectNow 决定要不要立刻建立 TCP 连接。
MprpcChannel::MprpcChannel(std::string ip, std::uint16_t port, bool connectNow,
                           std::chrono::milliseconds ioTimeout)
    : m_clientFd(-1), m_ip(std::move(ip)), m_port(port), m_ioTimeout(ioTimeout) {
  if (!connectNow) {//可以允许延迟连接
    return;
  }  
  std::string errMsg;
  auto rt = newConnect(ip.c_str(), port, &errMsg);
  int tryCount = 3;//重试三次
  while (!rt && tryCount--) {
    std::cout << errMsg << std::endl;
    rt = newConnect(ip.c_str(), port, &errMsg);
  }
}
