//启动 Raft KV 服务端集群的示例程序。
#include <iostream>
#include <filesystem>
#include <fstream>
#include "raft.h"
// #include "kvServer.h"
#include <kvServer.h>
#include <unistd.h>
#include <iostream>
#include <random>

void ShowArgsHelp();

int main(int argc, char **argv) {
  //////////////////////////////////读取命令参数：节点数量、写入raft节点节点信息到哪个文件
  if (argc < 2) {
    ShowArgsHelp();
    exit(EXIT_FAILURE);
  }
  int c = 0;
  int nodeNum = 0;
  std::string configFileName;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(10000, 29999);
  unsigned short startPort = dis(gen);
  while ((c = getopt(argc, argv, "n:f:")) != -1) {
    switch (c) {
      case 'n':
        nodeNum = atoi(optarg);
        break;
      case 'f':
        configFileName = optarg;
        break;
      default:
        ShowArgsHelp();
        exit(EXIT_FAILURE);
    }
  }
  if (nodeNum <= 0 || nodeNum > 10000 || configFileName.empty()) {
    ShowArgsHelp();
    return EXIT_FAILURE;
  }
  const std::filesystem::path configPath(configFileName);
  const std::filesystem::path dataRoot = configPath.parent_path() / "data";
  std::filesystem::create_directories(dataRoot);
  std::ofstream file(configPath, std::ios::out | std::ios::trunc);
  if (!file) {
    std::cerr << "无法打开 " << configFileName << std::endl;
    exit(EXIT_FAILURE);
  }
  for (int i = 0; i < nodeNum; ++i) {
    const auto port = static_cast<unsigned int>(startPort) + static_cast<unsigned int>(i);
    file << "node" << i << "ip=127.0.0.1\n";
    file << "node" << i << "port=" << port << '\n';
    std::filesystem::create_directories(dataRoot / ("node-" + std::to_string(i)));
  }
  file.close();

  for (int i = 0; i < nodeNum; i++) {
    std::cout << "start to create raftkv node:" << i << " pid:" << getpid() << std::endl;
    pid_t pid = fork();  // 创建新进程
    if (pid == 0) {
      try {
        KvServer server(i, 500, configPath, dataRoot / ("node-" + std::to_string(i)));
        server.StartKVServer();
        return 0;
      } catch (const std::exception& error) {
        std::cerr << "node " << i << ": " << error.what() << std::endl;
        return EXIT_FAILURE;
      }
    } else if (pid > 0) {
      continue;
    } else {
      // 如果创建进程失败
      std::cerr << "Failed to create child process." << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  pause();
  return 0;
}

void ShowArgsHelp() { std::cout << "format: command -n <nodeNum> -f <configFileName>" << std::endl; }
