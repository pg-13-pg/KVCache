#include "cluster_config.h"

#include <charconv>
#include <cctype>
#include <fstream>
#include <map>
#include <stdexcept>
#include <string_view>
#include <unordered_map>

namespace {

std::string Trim(std::string_view value) {
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
    value.remove_prefix(1);
  }
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back()))) {
    value.remove_suffix(1);
  }
  return std::string(value);
}

struct ParsedKey {
  int nodeId;
  bool isPort;
};

ParsedKey ParseKey(const std::string& key) {
  if (!key.starts_with("node")) {
    throw std::runtime_error("invalid cluster key: " + key);
  }
  const bool isIp = key.ends_with("ip");
  const bool isPort = key.ends_with("port");
  if (!isIp && !isPort) {
    throw std::runtime_error("invalid cluster key: " + key);
  }
  const std::size_t suffixSize = isPort ? 4 : 2;
  const auto idText = std::string_view(key).substr(4, key.size() - 4 - suffixSize);
  int nodeId = -1;
  const auto result = std::from_chars(idText.data(), idText.data() + idText.size(), nodeId);
  if (idText.empty() || result.ec != std::errc{} || result.ptr != idText.data() + idText.size() || nodeId < 0) {
    throw std::runtime_error("invalid cluster key: " + key);
  }
  return {nodeId, isPort};
}

std::uint16_t ParsePort(const std::string& key, const std::string& value) {
  unsigned int port = 0;
  const auto result = std::from_chars(value.data(), value.data() + value.size(), port);
  if (value.empty() || result.ec != std::errc{} || result.ptr != value.data() + value.size() ||
      port == 0 || port > 65535) {
    throw std::runtime_error("invalid port for " + key);
  }
  return static_cast<std::uint16_t>(port);
}

}  // namespace

std::vector<NodeEndpoint> LoadClusterConfig(const std::filesystem::path& path) {
  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error("cannot open cluster config: " + path.string());
  }

  std::unordered_map<std::string, std::string> values;
  std::map<int, bool> nodeIds;
  std::string line;
  while (std::getline(input, line)) {
    const std::string trimmed = Trim(line);
    if (trimmed.empty() || trimmed.front() == '#') continue;
    const auto separator = trimmed.find('=');
    if (separator == std::string::npos) {
      throw std::runtime_error("invalid cluster config line: " + trimmed);
    }
    const std::string key = Trim(std::string_view(trimmed).substr(0, separator));
    const std::string value = Trim(std::string_view(trimmed).substr(separator + 1));
    const auto parsed = ParseKey(key);
    if (!values.emplace(key, value).second) {
      throw std::runtime_error("duplicate cluster key: " + key);
    }
    nodeIds[parsed.nodeId] = true;
  }

  if (nodeIds.empty()) {
    throw std::runtime_error("missing cluster key: node0");
  }
  const int maxNodeId = nodeIds.rbegin()->first;
  std::vector<NodeEndpoint> endpoints;
  endpoints.reserve(static_cast<std::size_t>(maxNodeId + 1));
  for (int nodeId = 0; nodeId <= maxNodeId; ++nodeId) {
    const std::string prefix = "node" + std::to_string(nodeId);
    if (!nodeIds.contains(nodeId)) {
      throw std::runtime_error("missing cluster key: " + prefix);
    }
    const std::string ipKey = prefix + "ip";
    const std::string portKey = prefix + "port";
    const auto ip = values.find(ipKey);
    if (ip == values.end() || ip->second.empty()) {
      throw std::runtime_error("missing cluster key: " + ipKey);
    }
    const auto port = values.find(portKey);
    if (port == values.end()) {
      throw std::runtime_error("missing cluster key: " + portKey);
    }
    endpoints.push_back({ip->second, ParsePort(portKey, port->second)});
  }
  return endpoints;
}
