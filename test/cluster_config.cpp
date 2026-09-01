#include "cluster_config.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>

namespace fs = std::filesystem;

namespace {

fs::path WriteConfig(const fs::path& root, const std::string& name,
                     const std::string& contents) {
  const auto path = root / name;
  std::ofstream output(path);
  output << contents;
  return path;
}

void Expect(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

void ExpectFailure(const fs::path& path, const std::string& key) {
  try {
    (void)LoadClusterConfig(path);
  } catch (const std::runtime_error& error) {
    Expect(std::string(error.what()).find(key) != std::string::npos,
           "config error omitted the failing key");
    return;
  }
  throw std::runtime_error("malformed config was accepted");
}

}  // namespace

int main() {
  const auto root = fs::temp_directory_path() /
                    ("kvraft-config-" + std::to_string(
                        std::chrono::steady_clock::now().time_since_epoch().count()));
  fs::create_directories(root);
  try {
    const auto valid = WriteConfig(
        root, "valid.conf",
        "# static cluster\n node0ip = 127.0.0.1 \nnode0port=21000\n"
        "node1ip=127.0.0.1\nnode1port=21001\n"
        "node2ip=127.0.0.1\nnode2port=21002\n");
    const auto nodes = LoadClusterConfig(valid);
    Expect(nodes.size() == 3, "valid config returned wrong node count");
    Expect(nodes[0].ip == "127.0.0.1" && nodes[0].port == 21000,
           "node0 endpoint changed");
    Expect(nodes[2].port == 21002, "node ordering changed");

    ExpectFailure(WriteConfig(root, "missing.conf",
                              "node0ip=127.0.0.1\nnode0port=1\nnode1ip=127.0.0.1\n"),
                  "node1port");
    ExpectFailure(WriteConfig(root, "nonnumeric.conf",
                              "node0ip=127.0.0.1\nnode0port=abc\n"),
                  "node0port");
    ExpectFailure(WriteConfig(root, "zero.conf",
                              "node0ip=127.0.0.1\nnode0port=0\n"),
                  "node0port");
    ExpectFailure(WriteConfig(root, "large.conf",
                              "node0ip=127.0.0.1\nnode0port=65536\n"),
                  "node0port");
    ExpectFailure(WriteConfig(root, "gap.conf",
                              "node0ip=127.0.0.1\nnode0port=1\n"
                              "node2ip=127.0.0.1\nnode2port=2\n"),
                  "node1");
    ExpectFailure(WriteConfig(root, "duplicate.conf",
                              "node0ip=127.0.0.1\nnode0ip=127.0.0.2\nnode0port=1\n"),
                  "node0ip");
  } catch (...) {
    fs::remove_all(root);
    return 1;
  }
  fs::remove_all(root);
  return 0;
}
