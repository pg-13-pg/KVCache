#include "kvServer.h"

#include <rpcprovider.h>

#include "cluster_config.h"

void KvServer::DprintfKVDB() {
  if (!Debug) {
    return;
  }
  // Dumping the entire Skip List after every request makes Debug mode
  // unusable under load. Per-request apply logs already record the mutation.
}

void KvServer::ExecuteAppendOpOnKVDB(Op op) {
  m_mtx.lock();
  m_skipList.insert_set_element(op.Key, op.Value);
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();
  DprintfKVDB();
}

void KvServer::ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist) {
  m_mtx.lock();
  *value = "";
  *exist = false;
  if (m_skipList.search_element(op.Key, *value)) {
    *exist = true;
  }
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();
  DprintfKVDB();
}

void KvServer::ExecutePutOpOnKVDB(Op op) {
  m_mtx.lock();
  m_skipList.insert_set_element(op.Key, op.Value);
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();
  DprintfKVDB();
}
// 处理来自clerk的Get RPC入口
//发送给对应raft节点，只有leader节点（其他节点不处理）会广播，经多数确认后，raft节点（leader）会把这个op写入applyChan，
//其余节点是通过leader广播的commitIndex，然后 写入applyChan，来更新自己的状态机的，
void KvServer::Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply) {
  Op op;
  op.Operation = "Get";
  op.Key = args->key();
  op.Value = "";
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();

  int raftIndex = -1;
  int _ = -1;
  bool isLeader = false;
  m_raftNode->Start(op, &raftIndex, &_, &isLeader);  // raftIndex：raft预计的logIndex
  // ，虽然是预计，但是正确情况下是准确的，op的具体内容对raft来说 是隔离的

  if (!isLeader) {
    reply->set_err(ErrWrongLeader);
    return;
  }
  // create waitForCh
  m_mtx.lock();
  if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
    waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
  }////等待raft返回的applyChan消息，kvserver从applyChan中获取到这个op后执行真正的操作
  auto chForRaftIndex = waitApplyCh[raftIndex];
  m_mtx.unlock();  //直接解锁，等待任务执行完成，不能一直拿锁等待

  // timeout
  Op raftCommitOp;//接受raft返回的applyChan消息Op
  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) {//超时
    int _ = -1;
    bool isLeader = false;
    m_raftNode->GetState(&_, &isLeader);//重新检查是否是leader

    if (ifRequestDuplicate(op.ClientId, op.RequestId) && isLeader) {
      //如果超时，代表raft集群不保证已经commitIndex该日志，但是如果是已经提交过的get请求，是可以再执行的。
      // 不会违反线性一致性
      std::string value;
      bool exist = false;
      ExecuteGetOpOnKVDB(op, &value, &exist);
      if (exist) {
        reply->set_err(OK);
        reply->set_value(value);
      } else {
        reply->set_err(ErrNoKey);
        reply->set_value("");
      }
    } else {
      reply->set_err(ErrWrongLeader);  //返回这个，其实就是让clerk换一个节点重试
    }
  } else {
    // raft已经提交了该command（op），可以正式开始执行了
    //Leader可以会更换，需要检查raft返回的op和当前clerk发来的op是否一致，如果不一致，说明raft已经提交了一个新的op，当前clerk发来的op没有被提交
    if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
      std::string value;
      bool exist = false;
      ExecuteGetOpOnKVDB(op, &value, &exist);
      if (exist) {
        reply->set_err(OK);
        reply->set_value(value);
      } else {
        reply->set_err(ErrNoKey);
        reply->set_value("");
      }
    } else {
      reply->set_err(ErrWrongLeader);
    }
  }
  m_mtx.lock(); 
  auto tmp = waitApplyCh[raftIndex];
  waitApplyCh.erase(raftIndex);
  delete tmp;
  m_mtx.unlock();
}

//KVServer 收到 Raft 已提交的普通日志命令后，把它解析成 Op，执行到本地 KV 状态机，（Put 和 Append每个节点都需要执行）
//并通知正在等待的客户端 RPC 线程。
//Put/Append 是状态变更，必须在所有节点的 apply 阶段统一执行。
//Get 是线性一致读，只需要在 leader 确认提交后读一次，不需要在 apply 阶段真正执行。
void KvServer::GetCommandFromRaft(ApplyMsg message) {
  Op op;
  op.parseFromString(message.Command);  //反序列化，获取raft返回的applyChan消息中的op

  DPrintf(
      "[KvServer::GetCommandFromRaft-kvserver{%d}] , Got Command --> Index:{%d} , ClientId {%s}, RequestId {%d}, "
      "Opreation {%s}, Key :{%s}, Value :{%s}",
      m_me, message.CommandIndex, op.ClientId.c_str(), op.RequestId, op.Operation.c_str(), op.Key.c_str(),
      op.Value.c_str());
  if (message.CommandIndex <= m_lastSnapShotRaftLogIndex) {
    return;
  }
  if (op.Operation == "NoOp") {
    if (m_maxRaftState != -1) {
      IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
    }
    return;
  }
  if (!ifRequestDuplicate(op.ClientId, op.RequestId)) {
   
    if (op.Operation == "Put") {
      ExecutePutOpOnKVDB(op);
    }
    if (op.Operation == "Append") {
      ExecuteAppendOpOnKVDB(op);
    }
    
  }
  if (m_maxRaftState != -1) {
    IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
    //如果raft的log太大（大于指定的比例）就把制作快照
  }

  // Send message to the chan of op.ClientId
  SendMessageToWaitChan(op, message.CommandIndex);
}

//检查是否是重复的请求
bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId) {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_lastRequestId.find(ClientId) == m_lastRequestId.end()) {
    return false;
  }
  return RequestId <= m_lastRequestId[ClientId];
}

// PutAppend在收到raft消息之后执行，具体函数里面只判断幂等性（是否重复）
// get函数收到raft消息之后，因为get无论是否重复都可以再执行
void KvServer::PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply) {
  Op op;
  op.Operation = args->op();
  op.Key = args->key();
  op.Value = args->value();
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();
  int raftIndex = -1;
  int _ = -1;
  bool isleader = false;

  m_raftNode->Start(op, &raftIndex, &_, &isleader);

  if (!isleader) {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , but "
        "not leader",
        m_me, args->clientid().c_str(), args->requestid(), m_me, op.Key.c_str(), raftIndex);

    reply->set_err(ErrWrongLeader);
    return;
  }
  DPrintf(
      "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , is "
      "leader ",
      m_me, args->clientid().c_str(), args->requestid(), m_me, op.Key.c_str(), raftIndex);

  m_mtx.lock();
  if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) { 
    waitApplyCh.insert(std::make_pair(raftIndex, new LockQueue<Op>()));
  }
  auto chForRaftIndex = waitApplyCh[raftIndex];
  m_mtx.unlock();  //直接解锁，等待任务执行完成，不能一直拿锁等待

  Op raftCommitOp;
  if (!chForRaftIndex->timeOutPop(CONSENSUS_TIMEOUT, &raftCommitOp)) { //阻塞
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]TIMEOUT PUTAPPEND !!!! Server %d , get Command <-- Index:%d , "
        "ClientId %s, RequestId %d, Operation %s, Key :%s, Value :%s",
        m_me, m_me, raftIndex, op.ClientId.c_str(), op.RequestId, op.Operation.c_str(), op.Key.c_str(),
        op.Value.c_str());

    if (ifRequestDuplicate(op.ClientId, op.RequestId)) {
      reply->set_err(OK);  // 超时了,但因为是重复的请求，返回ok，实际上就算没有超时，在真正执行的时候也要判断是否重复
    } else {
      reply->set_err(ErrWrongLeader);  
    }
  } else {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]WaitChanGetRaftApplyMessage<--Server %d , get Command <-- Index:%d , "
        "ClientId %s, RequestId %d, Opreation %s, Key :%s, Value :%s",
        m_me, m_me, raftIndex, op.ClientId.c_str(), op.RequestId, op.Operation.c_str(), op.Key.c_str(),
        op.Value.c_str());
    if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId) {
      //可能发生leader的变更导致日志被覆盖，因此必须检查
      reply->set_err(OK);
    } else {
      reply->set_err(ErrWrongLeader);
    }
  }

  m_mtx.lock();

  auto tmp = waitApplyCh[raftIndex];
  waitApplyCh.erase(raftIndex);
  delete tmp;
  m_mtx.unlock();
}

//KVServer 的后台 apply 循环，监听 Raft 的 applyChan，执行已提交的日志命令或安装快照。
void KvServer::ReadRaftApplyCommandLoop() {
  while (true) {
    //如果只操作applyChan不用拿锁，因为applyChan自己带锁
    auto message = applyChan->Pop();  //阻塞弹出
    DPrintf("---------------tmp-------------[func-KvServer::ReadRaftApplyCommandLoop()-kvserver{%d}] 收到了下raft的消息", m_me);
    if (message.CommandValid) {
      GetCommandFromRaft(message);
    }
    if (message.SnapshotValid) {
      GetSnapShotFromRaft(message);
    }
  }
}

// raft与persist层交互，kvserver层也会和persist层交互，因为kvserver层开始的时候需要恢复kvdb的状态
//  关于快照，raft层与persist的交互：保存kvserver传来的snapshot；生成leaderInstallSnapshot RPC的时候也需要读取snapshot；
//  因此snapshot的具体格式是由kvserver层来定的，raft只负责传递这个东西
//  snapShot里面包含kvserver需要维护的persist_lastRequestId 以及kvDB真正保存的数据persist_kvdb
void KvServer::ReadSnapShotToInstall(std::string snapshot) {
  if (snapshot.empty()) {
    // bootstrap without any state?
    return;
  }
  parseFromString(snapshot); //反序列化
}

//把 Raft 已提交的 Op 发送到对应 raftIndex 的等待队列里，唤醒正在等结果的 RPC 线程。
bool KvServer::SendMessageToWaitChan(const Op &op, int raftIndex) {
  std::lock_guard<std::mutex> lg(m_mtx);
  DPrintf(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] Send Command --> Index:{%d} ClientId {%s}, RequestId "
      "{%d}, Operation {%s}, Key :{%s}, Value :{%s}",
      m_me, raftIndex, op.ClientId.c_str(), op.RequestId, op.Operation.c_str(), op.Key.c_str(), op.Value.c_str());

  if (waitApplyCh.find(raftIndex) == waitApplyCh.end()) {
    return false;
  }
  waitApplyCh[raftIndex]->Push(op);//push Op 到对应raftIndex的等待队列里，唤醒正在等结果的
  DPrintf(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] Send Command --> Index:{%d} ClientId {%s}, RequestId "
      "{%d}, Operation {%s}, Key :{%s}, Value :{%s}",
      m_me, raftIndex, op.ClientId.c_str(), op.RequestId, op.Operation.c_str(), op.Key.c_str(), op.Value.c_str());
  return true;
}

// 根据持久化RaftState大小来决定是否需要制作快照
void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion) {//
  if (m_raftNode->GetRaftStateSize() > m_maxRaftState / 10.0) {  //raft
    // Send SnapShot Command
    auto snapshot = MakeSnapShot();
    m_raftNode->Snapshot(raftIndex, snapshot); //raft::Snapshot层
  }
}

// 从 Raft 的 applyChan 中获取快照，并安装到 KVServer 的状态机中。
void KvServer::GetSnapShotFromRaft(ApplyMsg message) {
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_raftNode->CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot)) {//判断是否可安装（raft）
    ReadSnapShotToInstall(message.Snapshot); //安装快照，更新kvserver的状态机
    m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
  }
}


// 生成KVServer 状态机的快照  KVServer 状态机的快照，客户端请求去重信息
std::string KvServer::MakeSnapShot() {
  std::lock_guard<std::mutex> lg(m_mtx);
  std::string snapshotData = getSnapshotData();
  return snapshotData;
}
//重写RaftRpcProctoc::kvServerRpc的PutAppend和Get方法，提供给clerk远程调用
void KvServer::PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                         ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done) {
  KvServer::PutAppend(request, response); //业务处理
  done->Run();   //执行回调，返回响应
}

void KvServer::Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
                   ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done) {
  KvServer::Get(request, response);
  done->Run();
}

void KvServer::GetStatus(raftKVRpcProctoc::StatusReply *reply) {
  const auto status = m_raftNode->GetStatusSnapshot();
  reply->set_node_id(status.nodeId);
  reply->set_term(status.term);
  switch (status.role) {
    case RaftRole::Follower:
      reply->set_role(raftKVRpcProctoc::FOLLOWER);
      break;
    case RaftRole::Candidate:
      reply->set_role(raftKVRpcProctoc::CANDIDATE);
      break;
    case RaftRole::Leader:
      reply->set_role(raftKVRpcProctoc::LEADER);
      break;
  }
  reply->set_commit_index(status.commitIndex);
  reply->set_last_applied(status.lastApplied);
  reply->set_snapshot_index(status.snapshotIndex);
  reply->set_snapshot_term(status.snapshotTerm);
}

void KvServer::GetStatus(google::protobuf::RpcController *controller,
                         const ::raftKVRpcProctoc::StatusArgs *request,
                         ::raftKVRpcProctoc::StatusReply *response,
                         ::google::protobuf::Closure *done) {
  GetStatus(response);
  done->Run();
}

// 构造阶段只做配置校验和恢复，网络发布由 StartKVServer 显式启动。
KvServer::KvServer(int me, int maxRaftState, std::filesystem::path configPath,
                   std::filesystem::path dataDir, std::filesystem::path faultPolicy)
    : m_me(me), m_maxRaftState(maxRaftState), m_skipList(6), m_dataDir(std::move(dataDir)) {
  const auto endpoints = LoadClusterConfig(configPath);
  if (me < 0 || static_cast<std::size_t>(me) >= endpoints.size()) {
    throw std::runtime_error("node id is outside cluster config: " + std::to_string(me));
  }
  m_endpoint = endpoints[static_cast<std::size_t>(me)];
  auto persister = std::make_shared<Persister>(m_dataDir);

  applyChan = std::make_shared<LockQueue<ApplyMsg> >();
  m_raftNode = std::make_shared<Raft>();

  std::vector<std::shared_ptr<RaftRpcUtil> > servers;
  servers.reserve(endpoints.size());
  for (std::size_t i = 0; i < endpoints.size(); ++i) {
    if (i == m_me) {
      servers.push_back(nullptr);
      continue;
    }
    servers.push_back(std::make_shared<RaftRpcUtil>(endpoints[i].ip, endpoints[i].port, m_me,
                                                    static_cast<int>(i), faultPolicy));
  }
  m_raftNode->init(std::move(servers), m_me, persister, applyChan);
  m_lastSnapShotRaftLogIndex = m_raftNode->GetStatusSnapshot().snapshotIndex;
  auto snapshot = persister->ReadSnapshot();
  if (!snapshot.empty()) {
    ReadSnapShotToInstall(snapshot);
  }
  std::thread(&KvServer::ReadRaftApplyCommandLoop, this).detach();
}

void KvServer::StartKVServer() {
  m_raftNode->StartBackgroundTasks();
  std::thread(&KvServer::WritePeriodicStatusLog, this).detach();
  RpcProvider provider;
  provider.NotifyService(this);
  provider.NotifyService(m_raftNode.get());
  provider.Run(m_endpoint.ip, m_endpoint.port);
}

void KvServer::WritePeriodicStatusLog() {
  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    const auto status = m_raftNode->GetStatusSnapshot();
    std::error_code error;
    const auto walBytes = std::filesystem::file_size(m_dataDir / "raft.wal", error);
    const char* role = status.role == RaftRole::Leader ? "LEADER" :
                       status.role == RaftRole::Candidate ? "CANDIDATE" : "FOLLOWER";
    std::cout << "[status] node_id=" << m_me
              << " term=" << status.term
              << " role=" << role
              << " commit_index=" << status.commitIndex
              << " last_applied=" << status.lastApplied
              << " snapshot_index=" << status.snapshotIndex
              << " wal_bytes=" << (error ? 0 : walBytes) << std::endl;
  }
}
