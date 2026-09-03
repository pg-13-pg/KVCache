#pragma once

#include <cstdint>
#include <string>

namespace muduo {
namespace net {
class Buffer;
}
}

namespace mprpc {

constexpr std::uint32_t kMaxRpcFrameSize = 16 * 1024 * 1024;

std::string EncodeRpcFrame(const std::string& payload);
bool TryConsumeRpcFrame(muduo::net::Buffer* buffer, std::string* payload);

}  // namespace mprpc
