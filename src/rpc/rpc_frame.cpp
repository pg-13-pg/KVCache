#include "rpc_frame.h"

#include <arpa/inet.h>
#include <cstring>
#include <limits>
#include <stdexcept>

#include <muduo/net/Buffer.h>

namespace mprpc {

std::string EncodeRpcFrame(const std::string& payload) {
  if (payload.size() > kMaxRpcFrameSize || payload.size() > std::numeric_limits<std::uint32_t>::max()) {
    throw std::length_error("RPC frame payload exceeds maximum size");
  }
  const std::uint32_t length = htonl(static_cast<std::uint32_t>(payload.size()));
  std::string frame(sizeof(length), '\0');
  std::memcpy(frame.data(), &length, sizeof(length));
  frame.append(payload);
  return frame;
}

bool TryConsumeRpcFrame(muduo::net::Buffer* buffer, std::string* payload) {
  if (buffer == nullptr || payload == nullptr || buffer->readableBytes() < sizeof(std::uint32_t)) {
    return false;
  }
  std::uint32_t network_length = 0;
  std::memcpy(&network_length, buffer->peek(), sizeof(network_length));
  const std::uint32_t length = ntohl(network_length);
  if (length > kMaxRpcFrameSize ||
      buffer->readableBytes() < sizeof(network_length) + static_cast<std::size_t>(length)) {
    return false;
  }
  payload->assign(buffer->peek() + sizeof(network_length), length);
  buffer->retrieve(sizeof(network_length) + length);
  return true;
}

}  // namespace mprpc
