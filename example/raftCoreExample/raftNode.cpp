#include "kvServer.h"

#include <charconv>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

struct Arguments {
  int id = -1;
  int maxRaftState = -1;
  std::filesystem::path config;
  std::filesystem::path dataDir;
  std::filesystem::path raftFaultFile;
};

void PrintUsage(std::ostream& output) {
  output << "Usage: raftNode --id N --config PATH --data-dir PATH "
            "--max-raft-state BYTES [--raft-fault-file PATH]\n";
}

int ParseInteger(std::string_view value, const char* option) {
  int parsed = 0;
  const auto result = std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (value.empty() || result.ec != std::errc{} || result.ptr != value.data() + value.size()) {
    throw std::runtime_error(std::string("invalid value for ") + option);
  }
  return parsed;
}

Arguments ParseArguments(int argc, char** argv) {
  Arguments args;
  bool hasId = false;
  bool hasMaxRaftState = false;
  for (int index = 1; index < argc; ++index) {
    const std::string option = argv[index];
    if (option == "--help") {
      PrintUsage(std::cout);
      std::exit(0);
    }
    if (index + 1 >= argc) throw std::runtime_error("missing value for " + option);
    const std::string value = argv[++index];
    if (option == "--id") {
      args.id = ParseInteger(value, "--id");
      hasId = true;
    } else if (option == "--config") {
      args.config = value;
    } else if (option == "--data-dir") {
      args.dataDir = value;
    } else if (option == "--max-raft-state") {
      args.maxRaftState = ParseInteger(value, "--max-raft-state");
      hasMaxRaftState = true;
    } else if (option == "--raft-fault-file") {
      args.raftFaultFile = value;
    } else {
      throw std::runtime_error("unknown option: " + option);
    }
  }
  if (!hasId || args.config.empty() || args.dataDir.empty() || !hasMaxRaftState) {
    throw std::runtime_error("all four options are required");
  }
  if (args.id < 0) throw std::runtime_error("--id must be non-negative");
  if (args.maxRaftState <= 0) {
    throw std::runtime_error("--max-raft-state must be positive");
  }
  return args;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const auto args = ParseArguments(argc, argv);
    KvServer server(args.id, args.maxRaftState, args.config, args.dataDir, args.raftFaultFile);
    server.StartKVServer();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "raftNode: " << error.what() << '\n';
    PrintUsage(std::cerr);
    return 2;
  }
}
