#include "rpc_frame.h"

#include <cassert>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <string>
#include <vector>
#include <stdexcept>

#include <arpa/inet.h>

#include "kvServerRPC.pb.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "rpcheader.pb.h"

#include <muduo/net/Buffer.h>

namespace {

void SplitFrameWaitsForCompleteData() {
  const std::string encoded = mprpc::EncodeRpcFrame("hello");
  muduo::net::Buffer buffer;
  std::string payload;

  buffer.append(encoded.data(), 2);
  assert(!mprpc::TryConsumeRpcFrame(&buffer, &payload));
  assert(buffer.readableBytes() == 2);

  const std::size_t partialBodySize = sizeof(std::uint32_t) + 2;
  buffer.append(encoded.data() + 2, partialBodySize - 2);
  assert(!mprpc::TryConsumeRpcFrame(&buffer, &payload));
  assert(buffer.readableBytes() == partialBodySize);

  buffer.append(encoded.data() + partialBodySize, encoded.size() - partialBodySize);
  assert(mprpc::TryConsumeRpcFrame(&buffer, &payload));
  assert(payload == "hello");
  assert(buffer.readableBytes() == 0);
}

void CoalescedFramesAreConsumedOneAtATime() {
  const std::string first = mprpc::EncodeRpcFrame("one");
  const std::string second = mprpc::EncodeRpcFrame("two");
  muduo::net::Buffer buffer;
  buffer.append(first);
  buffer.append(second);
  std::string payload;

  assert(mprpc::TryConsumeRpcFrame(&buffer, &payload));
  assert(payload == "one");
  assert(buffer.readableBytes() == second.size());
  assert(mprpc::TryConsumeRpcFrame(&buffer, &payload));
  assert(payload == "two");
  assert(buffer.readableBytes() == 0);
}

void EmptyPayloadIsValid() {
  muduo::net::Buffer buffer;
  const std::string encoded = mprpc::EncodeRpcFrame("");
  buffer.append(encoded);
  std::string payload = "stale";

  assert(mprpc::TryConsumeRpcFrame(&buffer, &payload));
  assert(payload.empty());
  assert(buffer.readableBytes() == 0);
}

void OversizedFrameIsRejected() {
  muduo::net::Buffer buffer;
  const std::uint32_t oversized = htonl(mprpc::kMaxRpcFrameSize + 1);
  buffer.append(&oversized, sizeof(oversized));
  std::string payload;

  assert(!mprpc::TryConsumeRpcFrame(&buffer, &payload));
  assert(buffer.readableBytes() == sizeof(oversized));
}

void OversizedPayloadCannotBeEncoded() {
  bool rejected = false;
  try {
    (void)mprpc::EncodeRpcFrame(std::string(mprpc::kMaxRpcFrameSize + 1, 'x'));
  } catch (const std::length_error&) {
    rejected = true;
  }
  assert(rejected);
}

bool RecvExact(int fd, char* data, std::size_t size) {
  std::size_t received = 0;
  while (received < size) {
    const ssize_t result = recv(fd, data + received, size - received, 0);
    if (result <= 0) return false;
    received += static_cast<std::size_t>(result);
  }
  return true;
}

bool SendAll(int fd, const char* data, std::size_t size) {
  std::size_t sent = 0;
  while (sent < size) {
    const ssize_t result = send(fd, data + sent, size - sent, MSG_NOSIGNAL);
    if (result <= 0) return false;
    sent += static_cast<std::size_t>(result);
  }
  return true;
}

class LoopbackRpcServer {
 public:
  explicit LoopbackRpcServer(std::size_t requests) : requests_(requests) {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    assert(listen_fd_ >= 0);
    int reuse = 1;
    assert(setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) == 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    assert(bind(listen_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
    assert(listen(listen_fd_, 1) == 0);
    socklen_t address_size = sizeof(address);
    assert(getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&address), &address_size) == 0);
    port_ = ntohs(address.sin_port);
    thread_ = std::thread([this] { Serve(); });
  }

  ~LoopbackRpcServer() {
    if (thread_.joinable()) thread_.join();
    if (listen_fd_ >= 0) close(listen_fd_);
  }

  std::uint16_t port() const { return port_; }
  bool failed() const { return failed_.load(); }

 private:
  void Serve() {
    sockaddr_in peer{};
    socklen_t peer_size = sizeof(peer);
    const int client = accept(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &peer_size);
    if (client < 0) {
      failed_ = true;
      return;
    }
    for (std::size_t request = 0; request < requests_; ++request) {
      std::uint32_t network_size = 0;
      if (!RecvExact(client, reinterpret_cast<char*>(&network_size), sizeof(network_size))) {
        failed_ = true;
        break;
      }
      const std::uint32_t size = ntohl(network_size);
      if (size > mprpc::kMaxRpcFrameSize) {
        failed_ = true;
        break;
      }
      std::string body(size, '\0');
      if (!RecvExact(client, body.data(), body.size())) {
        failed_ = true;
        break;
      }

      // Decode the channel's varint RPC header, then the GetArgs payload.
      std::size_t cursor = 0;
      std::uint32_t header_size = 0;
      int shift = 0;
      while (cursor < body.size() && shift < 32) {
        const unsigned char byte = static_cast<unsigned char>(body[cursor++]);
        header_size |= static_cast<std::uint32_t>(byte & 0x7f) << shift;
        if ((byte & 0x80) == 0) break;
        shift += 7;
      }
      if (cursor + header_size > body.size()) {
        failed_ = true;
        break;
      }
      RPC::RpcHeader header;
      raftKVRpcProctoc::GetArgs args;
      if (!header.ParseFromArray(body.data() + cursor, header_size)) {
        failed_ = true;
        break;
      }
      cursor += header_size;
      if (cursor + header.args_size() > body.size() ||
          !args.ParseFromArray(body.data() + cursor, header.args_size())) {
        failed_ = true;
        break;
      }

      raftKVRpcProctoc::GetReply reply;
      reply.set_err("");
      if (args.key() == "large") {
        reply.set_value(std::string(4096, 'x'));
      } else {
        reply.set_value("reply:" + args.key());
      }
      const std::string frame = mprpc::EncodeRpcFrame(reply.SerializeAsString());
      // Force both prefix and body to arrive in multiple TCP reads.
      const std::size_t first = std::min<std::size_t>(2, frame.size());
      if (!SendAll(client, frame.data(), first)) {
        failed_ = true;
        break;
      }
      usleep(1000);
      for (std::size_t offset = first; offset < frame.size();) {
        const std::size_t chunk = std::min<std::size_t>(7, frame.size() - offset);
        if (!SendAll(client, frame.data() + offset, chunk)) {
          failed_ = true;
          break;
        }
        offset += chunk;
        usleep(500);
      }
      if (failed_) break;
    }
    close(client);
  }

  int listen_fd_ = -1;
  std::uint16_t port_ = 0;
  std::size_t requests_;
  std::atomic<bool> failed_{false};
  std::thread thread_;
};

class RecordingClosure final : public google::protobuf::Closure {
 public:
  explicit RecordingClosure(bool* called) : called_(called) {}
  void Run() override {
    *called_ = true;
    delete this;
  }
 private:
  bool* called_;
};

void FragmentedLargeResponseIsReadCompletely() {
  LoopbackRpcServer server(1);
  MprpcChannel channel("127.0.0.1", server.port(), false, std::chrono::milliseconds(2000));
  raftKVRpcProctoc::GetArgs request;
  request.set_key("large");
  raftKVRpcProctoc::GetReply response;
  MprpcController controller;
  channel.CallMethod(raftKVRpcProctoc::kvServerRpc::descriptor()->method(1),
                     &controller, &request, &response, nullptr);
  assert(!controller.Failed());
  assert(response.value().size() == 4096);
  assert(response.value()[0] == 'x');
  assert(!server.failed());
}

void DoneClosureRunsAfterSynchronousCall() {
  LoopbackRpcServer server(1);
  MprpcChannel channel("127.0.0.1", server.port(), false, std::chrono::milliseconds(2000));
  raftKVRpcProctoc::GetArgs request;
  request.set_key("done");
  raftKVRpcProctoc::GetReply response;
  MprpcController controller;
  bool called = false;
  channel.CallMethod(raftKVRpcProctoc::kvServerRpc::descriptor()->method(1),
                     &controller, &request, &response, new RecordingClosure(&called));
  assert(!controller.Failed());
  assert(called);
}

void OversizedResponseIsRejectedByChannel() {
  int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  assert(listen_fd >= 0);
  int reuse = 1;
  assert(setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) == 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  assert(bind(listen_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
  assert(listen(listen_fd, 1) == 0);
  socklen_t address_size = sizeof(address);
  assert(getsockname(listen_fd, reinterpret_cast<sockaddr*>(&address), &address_size) == 0);
  const auto port = ntohs(address.sin_port);

  std::thread server([listen_fd] {
    sockaddr_in peer{};
    socklen_t peer_size = sizeof(peer);
    const int client = accept(listen_fd, reinterpret_cast<sockaddr*>(&peer), &peer_size);
    assert(client >= 0);
    std::uint32_t request_size = 0;
    assert(RecvExact(client, reinterpret_cast<char*>(&request_size), sizeof(request_size)));
    const auto size = ntohl(request_size);
    std::string request(size, '\0');
    assert(RecvExact(client, request.data(), request.size()));
    const std::uint32_t oversized = htonl(mprpc::kMaxRpcFrameSize + 1);
    assert(SendAll(client, reinterpret_cast<const char*>(&oversized), sizeof(oversized)));
    close(client);
    close(listen_fd);
  });

  MprpcChannel channel("127.0.0.1", port, false, std::chrono::milliseconds(2000));
  raftKVRpcProctoc::GetArgs request;
  request.set_key("oversized-response");
  raftKVRpcProctoc::GetReply response;
  MprpcController controller;
  channel.CallMethod(raftKVRpcProctoc::kvServerRpc::descriptor()->method(1),
                     &controller, &request, &response, nullptr);
  assert(controller.Failed());
  assert(std::string(controller.ErrorText()).find("oversized") != std::string::npos);
  server.join();
}

void ConcurrentCallsOnOneChannelAreSerialized() {
  LoopbackRpcServer server(2);
  MprpcChannel channel("127.0.0.1", server.port(), false, std::chrono::milliseconds(2000));
  std::atomic<bool> failed{false};
  auto call = [&](const std::string& key) {
    raftKVRpcProctoc::GetArgs request;
    request.set_key(key);
    raftKVRpcProctoc::GetReply response;
    MprpcController controller;
    channel.CallMethod(raftKVRpcProctoc::kvServerRpc::descriptor()->method(1),
                       &controller, &request, &response, nullptr);
    if (controller.Failed() || response.value() != "reply:" + key) failed = true;
  };
  std::thread first(call, "first");
  std::thread second(call, "second");
  first.join();
  second.join();
  assert(!failed.load());
  assert(!server.failed());
}

}  // namespace

int main() {
  SplitFrameWaitsForCompleteData();
  CoalescedFramesAreConsumedOneAtATime();
  EmptyPayloadIsValid();
  OversizedFrameIsRejected();
  OversizedPayloadCannotBeEncoded();
  FragmentedLargeResponseIsReadCompletely();
  DoneClosureRunsAfterSynchronousCall();
  OversizedResponseIsRejectedByChannel();
  ConcurrentCallsOnOneChannelAreSerialized();
  return 0;
}
