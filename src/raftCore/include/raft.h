#ifndef RAFT_H
#define RAFT_H

#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "ApplyMsg.h"
#include "Persister.h"
#include "boost/any.hpp"
#include "boost/serialization/serialization.hpp"
#include "config.h"
#include "monsoon.h"
#include "raftRpcUtil.h"
#include "snapshot_policy.h"
#include "util.h"
constexpr int Disconnected =0; // 方便网络分区的时候debug，网络异常的时候为disconnected，只要网络正常就为AppNormal，防止matchIndex[]数组异常减小
constexpr int AppNormal = 1;

///////////////投票状态

constexpr int Killed = 0;
constexpr int Voted = 1;   //本轮已经投过票了
constexpr int Expire = 2;  //投票（消息、竞选者）过期
constexpr int Normal = 3;  //正常状态，可以投票

enum class RaftRole { Follower, Candidate, Leader };

struct RaftStatus {
  int nodeId;
  int term;
  RaftRole role;
  int commitIndex;
  int lastApplied;
  int snapshotIndex;
  int snapshotTerm;
};

class Raft : public raftRpcProctoc::raftRpc {  //raftRPC.proto
 private:
  std::mutex m_mtx;
  std::vector<std::shared_ptr<RaftRpcUtil>> m_peers;  //需要与其他raft节点通信,这里保存与其他节点通信的rpc入口
  std::shared_ptr<Persister> m_persister;  //持久层，负责raft数据的持久化
  int m_me;//raft是以集群启动，用来标识自己的编号
  int m_currentTerm;//记录当前的任期
  int m_votedFor;//记录当前term给谁投过票了，-1表示没有投过票
  std::vector<raftRpcProctoc::LogEntry> m_logs;  //// 日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号
                                                 // 这两个状态所有结点都在维护，易失
  int m_commitIndex;  //当前 Raft 节点已经确认提交的最高日志下标。
  int m_lastApplied;  // 已经汇报给状态机（上层应用）的log 的index

  // 这两个状态是由leader来维护
  std::vector<int> m_nextIndex;  //这个主要是 Leader 用的，对于每个 follower，leader 下一次应该从哪一条日志开始发送
  std::vector<int> m_matchIndex; //Leader 用的，leader 已经确认某个 follower 复制成功的最高日志下标
  enum Status { Follower, Candidate, Leader };
  Status m_status;  // 身份

  std::shared_ptr<LockQueue<ApplyMsg>> applyChan;  // client从这里取日志（2B），client与raft通信的接口
  // ApplyMsgQueue chan ApplyMsg // raft内部使用的chan，applyChan是用于和服务层交互，最后好像没用上

  // 选举超时

  std::chrono::_V2::system_clock::time_point m_lastResetElectionTime;//上一次重置选举计时器的时间点。
  // 心跳超时，用于leader
  std::chrono::_V2::system_clock::time_point m_lastResetHearBeatTime;//leader 上一次发送心跳的时间点。

  // 2D中用于传入快照点
  // 储存了快照中的最后一个日志的Index和Term
  int m_lastSnapshotIncludeIndex;
  int m_lastSnapshotIncludeTerm;

  // 协程
  std::unique_ptr<monsoon::IOManager> m_ioManager = nullptr;//调度心跳定时器和选举定时器。

 public:
  void AppendEntries1(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply);  // 处理leader发来的AppendEntries请求，包括心跳和日志复制
  void applierTicker();  // 周期性把已提交但未应用的日志推送给上层状态机
  bool CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot);  // 判断并安装上层传入的快照
  
  void doElection();  // 发起一轮新的leader选举
  void doHeartBeat();  // leader向其他节点发送心跳或日志复制请求
  void electionTimeOutTicker();  // 选举超时检测循环，超时后触发选举
  std::vector<ApplyMsg> getApplyLogs();  // 获取需要提交给状态机执行的日志消息
  int getNewCommandIndex();  // 获取新命令应该使用的日志index
  void getPrevLogInfo(int server, int *preIndex, int *preTerm);  // 获取发给指定节点的前一条日志信息
  void GetState(int *term, bool *isLeader);  // 查询当前任期以及本节点是否认为自己是leader
  RaftStatus GetStatusSnapshot();
  void InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest *args,
                       raftRpcProctoc::InstallSnapshotResponse *reply);  // 处理leader发来的安装快照请求
  void leaderHearBeatTicker();  // leader心跳定时循环
  void leaderSendSnapShot(int server);  // leader向指定follower发送快照
  void leaderUpdateCommitIndex();  // leader根据matchIndex推进commitIndex
  bool matchLog(int logIndex, int logTerm);  // 判断指定日志index和term是否与本地日志匹配
  void persist();  // 持久化当前Raft关键状态
  void RequestVote(const raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *reply);  // 处理候选人的投票请求
  bool UpToDate(int index, int term);  // 判断候选人日志是否至少和本节点一样新
  int getLastLogIndex();  // 获取当前最后一条日志的逻辑index
  int getLastLogTerm();  // 获取当前最后一条日志的term
  void getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm);  // 同时获取最后一条日志的index和term
  int getLogTermFromLogIndex(int logIndex);  // 根据逻辑日志index查询对应term
  int GetRaftStateSize();  // 获取当前持久化Raft状态大小
  int getSlicesIndexFromLogIndex(int logIndex);  // 将逻辑日志index转换为m_logs中的物理下标

  bool sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                       std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum);  // 向指定节点发送RequestVote RPC并处理响应
  bool sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                         std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply, std::shared_ptr<int> appendNums);  // 向指定节点发送AppendEntries RPC并处理响应

  // rf.applyChan <- msg //不拿锁执行  可以单独创建一个线程执行，但是为了同意使用std:thread
  // ，避免使用pthread_create，因此专门写一个函数来执行
  void pushMsgToKvServer(ApplyMsg msg);  // 将ApplyMsg推送给KvServer
  void readPersist(std::string data);  // 从持久化数据中恢复Raft状态
  std::string persistData();  // 将当前Raft状态序列化为字符串

  void Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader);  // 上层服务提交新命令到Raft日志
  // index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
  // 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
  // 即服务层主动发起请求raft保存snapshot里面的数据，index是用来表示snapshot快照执行到了哪条命令
  void Snapshot(int index, std::string snapshot);  // 上层服务通知Raft保存快照并截断已快照日志

 public:
  // 重写基类方法,因为rpc远程调用真正调用的是这个方法
  //序列化，反序列化等操作rpc框架都已经做完了，因此这里只需要获取值然后真正调用本地方法即可。
  void AppendEntries(google::protobuf::RpcController *controller, const ::raftRpcProctoc::AppendEntriesArgs *request,
                     ::raftRpcProctoc::AppendEntriesReply *response, ::google::protobuf::Closure *done) override;  // RPC框架回调入口，转发到AppendEntries1
  void InstallSnapshot(google::protobuf::RpcController *controller,
                       const ::raftRpcProctoc::InstallSnapshotRequest *request,
                       ::raftRpcProctoc::InstallSnapshotResponse *response, ::google::protobuf::Closure *done) override;  // RPC框架回调入口，处理InstallSnapshot请求
  void RequestVote(google::protobuf::RpcController *controller, const ::raftRpcProctoc::RequestVoteArgs *request,
                   ::raftRpcProctoc::RequestVoteReply *response, ::google::protobuf::Closure *done) override;  // RPC框架回调入口，处理RequestVote请求

 public:
  void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
            std::shared_ptr<LockQueue<ApplyMsg>> applyCh);  // 初始化Raft节点并启动后台定时任务

 private:
  // for persist

  class BoostPersistRaftNode {
   public:
    friend class boost::serialization::access;  //友元调用自定义的BoostPersistRaftNode数据结构的serialize函数，进行持久化
    // When the class Archive corresponds to an output archive, the
    // & operator is defined similar to <<.  Likewise, when the class Archive
    // is a type of input archive the & operator is defined similar to >>.
    template <class Archive>  //Archive： boost::archive::text_oarchive 或 boost::archive::text_iarchive
    void serialize(Archive &ar, const unsigned int version) {  // Boost序列化入口，保存和恢复Raft持久化字段
      ar &m_currentTerm;  // & 在 Boost 里不是普通按位与，含义是：写出时，类似 <<  类似 >>
      ar &m_votedFor;
      ar &m_lastSnapshotIncludeIndex;
      ar &m_lastSnapshotIncludeTerm;
      ar &m_logs;
    }
    int m_currentTerm;  // 持久化的当前任期
    int m_votedFor;  // 持久化的已投票节点
    int m_lastSnapshotIncludeIndex;  // 持久化的快照最后包含日志index
    int m_lastSnapshotIncludeTerm;  // 持久化的快照最后包含日志term
    std::vector<std::string> m_logs;  // 持久化的日志序列化数据  std::string：protobuf序列化后的字符串，方便持久化和网络传输
    std::unordered_map<std::string, int> umap;  // 预留的持久化map字段

   public:
  };
};

#endif  // RAFT_H
