#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

struct NodeEndpoint {
  std::string ip;
  std::uint16_t port;
};

std::vector<NodeEndpoint> LoadClusterConfig(const std::filesystem::path& path);
