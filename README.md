# KVCache

KVCache 是一个基于 Raft 的分布式键值存储实验项目。多个进程组成 Raft 集群，通过选主、日志复制和提交索引保证已提交数据的一致性；节点本地使用 WAL 和快照保存状态，重启后可以从数据目录恢复。项目还提供基于 Protobuf 的 RPC、客户端重试与故障转移、跳表键值引擎，以及用于测试丢包、延迟和重复消息的故障注入能力。

该项目适合学习和验证分布式存储核心机制，不是面向生产环境的完整数据库。当前版本不包含动态成员变更、分片、鉴权、加密传输和生产级运维能力。建议使用 3 个或更多节点，并使用奇数个节点以获得合理的法定人数。

## 目录结构

| 路径 | 作用 |
| --- | --- |
| src/raftCore | Raft 选举、日志复制、提交、快照、WAL 和故障策略 |
| src/rpc | Protobuf RPC、消息帧编解码和可靠的定长读写 |
| src/skipList | 内存键值状态机 |
| src/raftClerk | 客户端请求、重试和节点切换 |
| example/raftCoreExample | raftNode 服务端和 kvctl 客户端 |
| test | 单元测试、协议测试和多节点集成测试 |
| docs | 项目阅读、目录和测试说明 |

## 运行环境和依赖

项目当前按 Linux 环境编写，顶层 CMake 要求 CMake 3.22 或更高版本，并使用 C++20。推荐使用 Ubuntu 22.04/24.04 或兼容发行版，依赖包括：

- GCC 11+ 或其他支持 C++20 的编译器
- CMake >= 3.22
- Protobuf 编译器和运行库（protoc、libprotobuf）
- Muduo 网络库（muduo_net、muduo_base）
- Boost.Serialization
- pthread 和 dl
- Python 3（运行多节点集成测试）

Ubuntu/Debian 可以先安装常见依赖：

~~~bash
sudo apt update
sudo apt install -y build-essential cmake protobuf-compiler libprotobuf-dev \
  libboost-serialization-dev python3
~~~

Muduo 通常需要单独编译安装。当前工程默认从 /usr/local/include 查找头文件，从 /usr/local/lib 查找库文件；如果依赖安装在其他目录，需要调整 CMakeLists.txt 中的 include/library 路径，或将库安装到上述目录。

安装后可检查版本和库是否可见：

~~~bash
cmake --version
g++ --version
protoc --version
ldconfig -p | grep -E 'libprotobuf|muduo|boost_serialization'
~~~

## 配置集群

节点配置文件由 node<N>ip 和 node<N>port 两类键组成。节点 ID 必须从 0 连续编号，不能重复或缺失；端口必须在 1 到 65535 之间，每个节点使用唯一端口。空行和以 # 开头的行会被忽略。

例如创建 cluster.conf：

~~~text
# three-node local cluster
node0ip=127.0.0.1
node0port=19000
node1ip=127.0.0.1
node1port=19001
node2ip=127.0.0.1
node2port=19002
~~~

如果节点运行在不同服务器上，将 IP 替换为对等节点可访问的地址，并确保防火墙放行对应端口。所有节点和 kvctl 必须使用同一份配置。

## 编译

在项目根目录执行：

~~~bash
cmake -S . -B cmake-build-debug \
  -DCMAKE_BUILD_TYPE=Debug \
  -DBUILD_TESTING=ON
cmake --build cmake-build-debug --parallel "$(nproc)"
~~~

可执行文件会生成在 bin/，库文件会生成在 lib/。当前顶层 CMake 将构建类型固定为 Debug；如需 Release 优化，需要先调整顶层 CMake 的构建类型设置。不同编译配置建议使用不同构建目录，避免覆盖已有产物。

## 启动服务端

每个节点使用独立的数据目录，并在单独的终端中启动。--max-raft-state 是触发日志压缩/快照的状态大小上限，必须为正整数。

~~~bash
mkdir -p data/node-0 data/node-1 data/node-2

./bin/raftNode --id 0 --config cluster.conf \
  --data-dir data/node-0 --max-raft-state 1048576

./bin/raftNode --id 1 --config cluster.conf \
  --data-dir data/node-1 --max-raft-state 1048576

./bin/raftNode --id 2 --config cluster.conf \
  --data-dir data/node-2 --max-raft-state 1048576
~~~

完整参数格式：

~~~text
raftNode --id N --config PATH --data-dir PATH --max-raft-state BYTES \
         [--raft-fault-file PATH]
~~~

--raft-fault-file 仅用于测试故障注入。策略文件每行格式为 source target method action [milliseconds]，例如：

~~~text
any any AppendEntries drop
0 1 RequestVote delay 50
2 0 RequestVote duplicate
~~~

可用方法包括 AppendEntries、InstallSnapshot 和 RequestVote；动作包括 drop、delay 和 duplicate。不要在生产环境启用故障策略。

节点重启时应继续使用原来的数据目录，以便加载 WAL 和快照；要创建全新节点，请使用新的空目录。

## 使用客户端

kvctl 会根据配置连接节点，并在请求失败时尝试其他节点。超时时间必须为正数，单位是毫秒。

~~~bash
# 写入
./bin/kvctl --config cluster.conf --timeout-ms 5000 \
  put hello world

# 读取
./bin/kvctl --config cluster.conf --timeout-ms 5000 \
  get hello

# 查看指定节点状态
./bin/kvctl --config cluster.conf --timeout-ms 1000 \
  status --node 0
~~~

命令格式如下：

~~~text
kvctl --config PATH --timeout-ms N put KEY VALUE
kvctl --config PATH --timeout-ms N get KEY
kvctl --config PATH --timeout-ms N status --node N
~~~

status 用于查看节点 ID、任期、角色、提交索引、已应用索引和快照索引等状态。写请求只有在 Raft 集群完成提交后才会成功；没有多数节点在线时，写入可能超时。

## 测试

配置并编译时打开 BUILD_TESTING=ON 后，可以运行完整测试套件：

~~~bash
ctest --test-dir cmake-build-debug --output-on-failure
~~~

只运行 Raft 集成测试：

~~~bash
ctest --test-dir cmake-build-debug -L raft_integration --output-on-failure
~~~

重点协议和 Raft 测试：

~~~bash
ctest --test-dir cmake-build-debug \
  -R 'rpc_frame_test|raft_append_entries_test|raft_fault_policy_test' \
  --output-on-failure
~~~

集成测试会在本机启动 3 个 raftNode 进程，动态申请回环地址端口，并覆盖选主、读写、故障恢复、快照和 WAL 恢复等场景。重复运行可用于发现时序问题：

~~~bash
ctest --test-dir cmake-build-debug \
  -L raft_integration --repeat until-fail:10 --output-on-failure
~~~

集成测试失败时会保留测试产物和节点日志，具体目录以测试输出为准，通常位于构建目录下的 test/artifacts/。

## 常见问题

- 找不到 Muduo、Protobuf 或 Boost 库：确认库文件和头文件位于工程当前查找路径，或修改 CMakeLists.txt 中的路径后重新配置。
- 配置文件解析失败：检查节点 ID 是否从 0 连续、每个节点是否同时存在 IP 和端口、端口是否有效且未重复。
- 端口已被占用：更换 cluster.conf 中的端口，并确保旧的 raftNode 进程已退出。
- 节点无法形成集群：确认所有节点使用同一配置、地址可达，并且至少有多数节点正常运行。
- 重启后数据不一致：确认没有误删或更换节点数据目录；恢复依赖目录中的 WAL 和快照文件。
- Sanitizer 或多套构建相互覆盖：由于产物目录是项目级的 bin/ 和 lib/，请为不同配置使用隔离工作树或在切换配置后重新编译。

## 快速开始

~~~bash
cmake -S . -B cmake-build-debug -DBUILD_TESTING=ON
cmake --build cmake-build-debug --parallel "$(nproc)"
cat > cluster.conf <<'EOF'
node0ip=127.0.0.1
node0port=19000
node1ip=127.0.0.1
node1port=19001
node2ip=127.0.0.1
node2port=19002
EOF
mkdir -p data/node-0 data/node-1 data/node-2
~~~

然后按上面的服务端和客户端示例，在不同终端启动 3 个节点并执行 kvctl put/get。
