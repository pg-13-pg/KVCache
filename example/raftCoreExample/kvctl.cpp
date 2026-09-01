#include "clerk.h"
#include "cluster_config.h"
#include "raftServerRpcUtil.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

constexpr int kSuccess = 0;
constexpr int kMissingKey = 2;
constexpr int kUnavailable = 3;
constexpr int kUsage = 64;

void PrintUsage(std::ostream& output) {
  output << "Usage:\n"
            "  kvctl --config PATH --timeout-ms N put KEY VALUE\n"
            "  kvctl --config PATH --timeout-ms N get KEY\n"
            "  kvctl --config PATH --timeout-ms N status --node N\n";
}

int ParseInteger(std::string_view value, const char* option) {
  int parsed = 0;
  const auto result = std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (value.empty() || result.ec != std::errc{} || result.ptr != value.data() + value.size()) {
    throw std::runtime_error(std::string("invalid value for ") + option);
  }
  return parsed;
}

const char* RoleName(raftKVRpcProctoc::NodeRole role) {
  switch (role) {
    case raftKVRpcProctoc::FOLLOWER:
      return "FOLLOWER";
    case raftKVRpcProctoc::CANDIDATE:
      return "CANDIDATE";
    case raftKVRpcProctoc::LEADER:
      return "LEADER";
    default:
      throw std::runtime_error("unknown node role");
  }
}

int Run(int argc, char** argv) {
  if (argc == 2 && std::string_view(argv[1]) == "--help") {
    PrintUsage(std::cout);
    return kSuccess;
  }

  std::string config;
  int timeoutMs = -1;
  int index = 1;
  while (index < argc) {
    const std::string option = argv[index];
    if (option != "--config" && option != "--timeout-ms") break;
    if (++index >= argc) throw std::runtime_error("missing value for " + option);
    if (option == "--config") {
      config = argv[index++];
    } else {
      timeoutMs = ParseInteger(argv[index++], "--timeout-ms");
    }
  }
  if (config.empty() || timeoutMs <= 0 || index >= argc) {
    throw std::runtime_error("config, positive timeout, and command are required");
  }

  const std::string command = argv[index++];
  const auto requestedTimeout = std::chrono::milliseconds(timeoutMs);
  const auto ioTimeout = std::min(requestedTimeout, std::chrono::milliseconds(300));
  const auto deadline = std::chrono::steady_clock::now() + requestedTimeout;
  if (command == "put") {
    if (index + 2 != argc) throw std::runtime_error("put requires KEY VALUE");
    Clerk clerk;
    clerk.Init(config, ioTimeout);
    const auto status = clerk.PutUntil(argv[index], argv[index + 1], deadline);
    return status == ClerkStatus::Ok ? kSuccess : kUnavailable;
  }
  if (command == "get") {
    if (index + 1 != argc) throw std::runtime_error("get requires KEY");
    Clerk clerk;
    clerk.Init(config, ioTimeout);
    std::string value;
    const auto status = clerk.GetUntil(argv[index], &value, deadline);
    if (status == ClerkStatus::NotFound) return kMissingKey;
    if (status != ClerkStatus::Ok) return kUnavailable;
    std::cout << value << '\n';
    return kSuccess;
  }
  if (command == "status") {
    if (index + 2 != argc || std::string_view(argv[index]) != "--node") {
      throw std::runtime_error("status requires --node N");
    }
    const int node = ParseInteger(argv[index + 1], "--node");
    const auto endpoints = LoadClusterConfig(config);
    if (node < 0 || static_cast<std::size_t>(node) >= endpoints.size()) {
      throw std::runtime_error("--node is outside cluster config");
    }
    raftServerRpcUtil rpc(endpoints[static_cast<std::size_t>(node)].ip,
                          endpoints[static_cast<std::size_t>(node)].port, ioTimeout);
    raftKVRpcProctoc::StatusReply reply;
    if (!rpc.GetStatus(&reply)) return kUnavailable;
    std::cout << "node_id=" << reply.node_id() << " term=" << reply.term()
              << " role=" << RoleName(reply.role())
              << " commit_index=" << reply.commit_index()
              << " last_applied=" << reply.last_applied()
              << " snapshot_index=" << reply.snapshot_index()
              << " snapshot_term=" << reply.snapshot_term() << '\n';
    return kSuccess;
  }
  throw std::runtime_error("unknown command: " + command);
}

}  // namespace

int main(int argc, char** argv) {
  try {
    return Run(argc, argv);
  } catch (const std::exception& error) {
    std::cerr << "kvctl: " << error.what() << '\n';
    PrintUsage(std::cerr);
    return kUsage;
  }
}
