
#ifndef SKIP_LIST_ON_RAFT_KVSERVER_H
#define SKIP_LIST_ON_RAFT_KVSERVER_H

#include <boost/any.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/foreach.hpp>
#include <boost/serialization/export.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/vector.hpp>
#include <iostream>
#include <filesystem>
#include <mutex>
#include <unordered_map>
#include "cluster_config.h"
#include "kvServerRPC.pb.h"
#include "raft.h"
#include "skipList.h"

class KvServer : raftKVRpcProctoc::kvServerRpc {
 private:
  std::mutex m_mtx;
  int m_me;
  std::shared_ptr<Raft> m_raftNode;  // kvServer和raft节点的通信接口
  std::shared_ptr<LockQueue<ApplyMsg> > applyChan;  // raft节点向上层kvServer传递已提交的日志或快照的消息队列
  int m_maxRaftState;                               //快照触发阈值

  std::string m_serializedKVData;  //临时保存 m_skipList 序列化后的 KV 数据，让 Boost 能把它一起写进 snapshot
  SkipList<std::string, std::string> m_skipList;  // kv数据的存储结构，使用跳表实现，提供高效的读写性能

  //std::unordered_map<std::string, std::string> m_kvDB; 

  //发送日志给raft后，kvserver需要等待raft返回的applyChan消息，才能知道raft是否已经提交了这个日志
  std::unordered_map<int, LockQueue<Op> *> waitApplyCh; 
  std::unordered_map<std::string, int> m_lastRequestId;  // clientid -> requestID  //一个kV服务器可能连接多个client

  int m_lastSnapShotRaftLogIndex;   //快照包含的最后一个日志索引
  NodeEndpoint m_endpoint;

 public:
  KvServer() = delete;
  KvServer(int me, int maxRaftState, std::filesystem::path configPath,
           std::filesystem::path dataDir);

  void StartKVServer();
  void DprintfKVDB();

  void ExecuteAppendOpOnKVDB(Op op);  //真正操作本地kv数据库的函数
  void ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist);
  void ExecutePutOpOnKVDB(Op op);
  
  void GetCommandFromRaft(ApplyMsg message);
  bool ifRequestDuplicate(std::string ClientId, int RequestId);

  // clerk 使用RPC远程调用
  void PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply);
  //将 GetArgs 改为rpc调用的，因为是远程客户端，即服务器宕机对客户端来说是无感的
  void Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply);  
  void GetStatus(raftKVRpcProctoc::StatusReply *reply);
  
  //一直等待raft传来的applyChan，然后执行applyChan中的命令，并且把结果返回给等待的线程
  void ReadRaftApplyCommandLoop();
  void ReadSnapShotToInstall(std::string snapshot);//安装快照，更新kvserver的状态机
  bool SendMessageToWaitChan(const Op &op, int raftIndex);//把raft返回的applyChan消息发送给等待的线程
  void GetSnapShotFromRaft(ApplyMsg message);//从raft传来的applyChan中获取快照，并且安装快照

  // 检查是否需要制作快照，需要的话就向raft之下制作快照
  void IfNeedToSendSnapShotCommand(int raftIndex, int proportion);

  std::string MakeSnapShot();//制作快照，返回快照的序列化字符串

 public:  // for rpc
  void PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                 ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done) override;

  void Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
           ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done) override;

  void GetStatus(google::protobuf::RpcController *controller,
                 const ::raftKVRpcProctoc::StatusArgs *request,
                 ::raftKVRpcProctoc::StatusReply *response,
                 ::google::protobuf::Closure *done) override;

  
 private:
  friend class boost::serialization::access;

  template <class Archive>
  void serialize(Archive &ar, const unsigned int version) 
  {
    ar &m_serializedKVData;
    ar &m_lastRequestId;
  }

//把当前 KVServer 状态机的状态序列化成 snapshot 字符串
  std::string getSnapshotData() {
    m_serializedKVData = m_skipList.dump_file();
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);  //保存到ss
    oa << *this;
    m_serializedKVData.clear();
    return ss.str();  
  }
//把 snapshot 字符串反序列化成 KVServer 状态机的状态
  void parseFromString(const std::string &str) { 
    std::stringstream ss(str);
    boost::archive::text_iarchive ia(ss);
    ia >> *this;
    m_skipList.load_file(m_serializedKVData);  //把序列化的kv数据加载到跳表中
    m_serializedKVData.clear();                 
  }

};

#endif  
