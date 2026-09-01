#include "raft.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <memory>
#include "config.h"
#include "util.h"

//心跳或日志复制请求  更新日志  1.检查term 2.检查index 3.检查index相同 term是否相同
void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs* args, raftRpcProctoc::AppendEntriesReply* reply) {
  std::lock_guard<std::mutex> locker(m_mtx);
  reply->set_appstate(AppNormal);  // 能接收到代表网络是正常的
  //	不同的人收到AppendEntries的反应是不同的，要注意无论什么时候收到rpc请求和响应都要检查term
  if (args->term() < m_currentTerm) {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(-100);  // 论文中：让领导人可以及时更新自己
    DPrintf("[func-AppendEntries-rf{%d}] 拒绝了 因为Leader{%d}的term{%v}< rf{%d}.term{%d}\n", m_me, args->leaderid(),
            args->term(), m_me, m_currentTerm);
    return;  
  }

  bool persistentStateChanged = false;
  DEFER {
    if (persistentStateChanged) persist();
    m_lastResetElectionTime = now();
  };

  if (args->term() > m_currentTerm) {
    // 三变 ,防止遗漏，无论什么时候都是三变
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;  // 这里设置成-1有意义，如果突然宕机然后上线理论上是可以投票的
    persistentStateChanged = true;
    // 这里可不直接返回，应该改成让改节点尝试接收日志
    // 如果是领导人和candidate突然转到Follower好像也不用其他操作
    // 如果本来就是Follower，那么其term变化，相当于“不言自明”的换了追随的对象，因为原来的leader的term更小，是不会再接收其消息了
  }
  myAssert(args->term() == m_currentTerm, format("assert {args.Term == rf.currentTerm} fail"));
  // 如果发生网络分区，那么candidate可能会收到同一个term的leader的消息，要转变为Follower
  m_status = Follower;  

  // 不能无脑的从prevlogIndex开始截断修改日志，因为rpc可能会延迟，导致发过来的rpc ae是很久之前的，（leader日志太久）
  //	那么就比较日志，日志有3种情况
  if (args->prevlogindex() > getLastLogIndex()) {  //follower日志没有那么长，缺少日志，回退
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(getLastLogIndex() + 1);
    return;
  } else if (args->prevlogindex() < m_lastSnapshotIncludeIndex) {  // leader日志太旧，follower已经快照了，回退到快照的后一条日志
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(m_lastSnapshotIncludeIndex +1); // 你至少从 snapshot 后面的第一条日志开始试。
    //return;
  }

  //	本机日志有那么长，冲突(same index,different term),截断日志
  // 注意：这里目前当args.PrevLogIndex == rf.lastSnapshotIncludeIndex与不等的时候要分开考虑，可以看看能不能优化这块
  if (matchLog(args->prevlogindex(), args->prevlogterm())) {//leader的args->prevlogterm和follower的prevlogindex对应的日志term是否匹配，
    //	todo：	整理logs
    //不能直接截断，必须一个一个检查，因为发送来的log可能是之前的，直接截断可能导致“取回”已经在follower日志中的条目
    // 可能会有一段发来的AE中的logs中前半是匹配的，后半是不匹配的，这种应该：1.follower如何处理？ 2.如何给leader回复
    // 3. leader如何处理
    //leader  123 456（entries）  follower 12345678  456已经存在follower中，leader发来的456是旧的，follower应该直接忽略掉，而不是截断掉follower的日志
    for (int i = 0; i < args->entries_size(); i++) {
      auto log = args->entries(i);
      if (log.logindex() > getLastLogIndex()) {//leader更新日志比follwer最新日志LastLogIndex新
        
        m_logs.push_back(log);
        persistentStateChanged = true;
      } else {//这条日志的 index follower 已经拥有
        // todo ： 这里可以改进为比较对应logIndex位置的term是否相等，term相等就代表匹配
        //  todo：这个地方放出来会出问题,按理说index相同，term相同，log也应该相同才对
        // rf.logs[entry.Index-firstIndex].Term ?= entry.Term
        //1.term不匹配就更新 2.term相同但是command不匹配就异常了 3.term和command匹配不进行操作
        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() == log.logterm() &&
            m_logs[getSlicesIndexFromLogIndex(log.logindex())].command() != log.command()) {
          //相同位置的log ，其logTerm相等，但是命令却不相同，不符合raft的前向匹配，异常了！
          myAssert(false, format("[func-AppendEntries-rf{%d}] 两节点logIndex{%d}和term{%d}相同，但是其command{%d:%d}   "
                                 " {%d:%d}却不同！！\n",
                                 m_me, log.logindex(), log.logterm(), m_me,
                                 m_logs[getSlicesIndexFromLogIndex(log.logindex())].command(), args->leaderid(),
                                 log.command()));
        }
        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() != log.logterm()) {
          //term不匹配就更新
          m_logs[getSlicesIndexFromLogIndex(log.logindex())] = log;
          persistentStateChanged = true;
        }
      }
    }
    //这次 AppendEntries 覆盖到的最后一个日志 index。
    myAssert(
        getLastLogIndex() >= args->prevlogindex() + args->entries_size(),
        format("[func-AppendEntries1-rf{%d}]rf.getLastLogIndex(){%d} != args.PrevLogIndex{%d}+len(args.Entries){%d}",
               m_me, getLastLogIndex(), args->prevlogindex(), args->entries_size()));
    //follwer更新  m_commitIndex         
    if (args->leadercommit() > m_commitIndex) {//leader的commitIndex比follower的commitIndex大，follower更新m_commitIndex
      m_commitIndex = std::min(args->leadercommit(), getLastLogIndex()); // 可能存在args->leadercommit()落后于 getLastLogIndex()的情况
    }

    myAssert(getLastLogIndex() >= m_commitIndex,//Follower 的 commitIndex 不能超过自己本地最后一条日志 index。
             format("[func-AppendEntries1-rf{%d}]  rf.getLastLogIndex{%d} < rf.commitIndex{%d}", m_me,
                    getLastLogIndex(), m_commitIndex));
    reply->set_success(true);
    reply->set_term(m_currentTerm);
    return;
  }
  //log的index相等，term不相等 不匹配 
   else {
    // PrevLogIndex 长度合适，但是不匹配，因此往前寻找矛盾的term的第一个元素，不一个个回退，减少rpc
    // ？什么时候term会矛盾呢？很多情况，比如leader接收了日志之后马上就崩溃等等
    reply->set_updatenextindex(args->prevlogindex());

    for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index) {
      if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex())) {//term不匹配，继续往前找
        reply->set_updatenextindex(index + 1);
        break;
      }
    }
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    return;
  }
}

//周期性把已提交但未应用的日志推送给上层状态机 ，通过applyChan这个线程安全的队列交付给上层状态机/KVServer
void Raft::applierTicker() {
  while (true) {
    m_mtx.lock();
    if (m_status == Leader) {
      DPrintf("[Raft::applierTicker() - raft{%d}]  m_lastApplied{%d}   m_commitIndex{%d}", m_me, m_lastApplied,
              m_commitIndex);
    }
    auto applyMsgs = getApplyLogs();
    m_mtx.unlock();
    //因为 applyChan->Push() 可能慢，甚至阻塞。如果拿着 Raft 的锁去 Push，就会卡住其他 Raft 逻辑，比如心跳、投票、日志复制
    // todo:好像必须拿锁，因为不拿锁的话如果调用多次applyLog函数，可能会导致应用的顺序不一样
    if (!applyMsgs.empty()) {
      DPrintf("[func- Raft::applierTicker()-raft{%d}] 向kvserver报告的applyMsgs长度为：{%d}", m_me, applyMsgs.size());
    }
    for (auto& message : applyMsgs) {
      applyChan->Push(message);  //applyChan是一个线程安全的队列，Raft往里面放日志，kvserver从里面取日志执行，因此这里不需要拿锁
    }
    sleepNMilliseconds(ApplyInterval);
  }
}
//todo：判断是否可以安装快照，待优化
bool Raft::CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot) {
  std::lock_guard<std::mutex> lock(m_mtx);
  return lastIncludedIndex == m_lastSnapshotIncludeIndex &&
         lastIncludedTerm == m_lastSnapshotIncludeTerm;
}


void Raft::doElection(std::chrono::system_clock::time_point observedResetTime) {
  std::lock_guard<std::mutex> g(m_mtx); //需要对raft节点状态进行加锁，比如m_status，m_currentTerm，m_votedFor等，因为这些状态在选举过程中会发生变化，
                                       //且选举过程是由定时器触发的，因此需要加锁保证线程安全

  // AppendEntries may reset the timer after the ticker wakes but before this
  // method acquires the lock. Do not turn that fresh follower into a candidate.
  if (m_status != Leader && m_lastResetElectionTime == observedResetTime) {
    DPrintf("[       ticker-func-rf(%d)              ]  选举定时器到期且不是leader，开始选举 \n", m_me);
    //当选举的时候定时器超时就必须重新选举，不然没有选票就会一直卡住
    //重竞选超时，term也会增加的
    m_status = Candidate;
    ///开始新一轮的选举
    m_currentTerm += 1;
    m_votedFor = m_me;  //即是自己给自己投，也避免candidate给同辈的candidate投
    persist();  //持久化节点状态
    std::shared_ptr<int> votedNum = std::make_shared<int>(1);  // 使用 make_shared 函数初始化 
    //	重新设置定时器
    m_lastResetElectionTime = now();
    //	发布RequestVote RPC
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        continue;
      }
      int lastLogIndex = -1, lastLogTerm = -1;
      getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);  //获取最后一个log的term和下标

      std::shared_ptr<raftRpcProctoc::RequestVoteArgs> requestVoteArgs =
          std::make_shared<raftRpcProctoc::RequestVoteArgs>();
      requestVoteArgs->set_term(m_currentTerm);
      requestVoteArgs->set_candidateid(m_me);
      requestVoteArgs->set_lastlogindex(lastLogIndex);
      requestVoteArgs->set_lastlogterm(lastLogTerm);
      auto requestVoteReply = std::make_shared<raftRpcProctoc::RequestVoteReply>();

      //使用匿名函数执行避免其拿到锁

      std::thread t(&Raft::sendRequestVote, this, i, requestVoteArgs, requestVoteReply,
                    votedNum);  // 创建新线程并执行b函数，并传递参数
      t.detach();
    }
  }
}


//心跳定时器触发，leader向所有follower发送AE来维护心跳和保持日志同步
void Raft::doHeartBeat() {
  std::lock_guard<std::mutex> g(m_mtx);

  if (m_status == Leader) {
    DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了且拿到mutex，开始发送AE\n", m_me);
    auto appendNums = std::make_shared<int>(1);  //正确返回的节点的数量

    //对Follower（除了自己外的所有节点发送AE）
    // todo 这里肯定是要修改的，最好使用一个单独的goruntime来负责管理发送log，因为后面的log发送涉及优化之类的
    //最少要单独写一个函数来管理，而不是在这一坨
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        continue;
      }
      DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了 index:{%d}\n", m_me, i);
      myAssert(m_nextIndex[i] >= 1, format("rf.nextIndex[%d] = {%d}", i, m_nextIndex[i])); //nextIndex[i] 不会小于 1
      //日志压缩加入后要判断是发送快照还是发送AE(日志)
      if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex) { //这里要发送的日志已经被保存到快照中了，因此要发送快照
        std::thread t(&Raft::leaderSendSnapShot, this, i);  // 创建新线程并执行leaderSendSnapShot函数
        t.detach();
        continue;
      }
      // 发送AE
      //构造发送值
      int preLogIndex = -1; //AppendEntries 的前一条 日志索引 用来给follower判断是否接受新的AE，可让leader回退m_nextIndex[i]
      int PrevLogTerm = -1; //AppendEntries 的前一条term
      getPrevLogInfo(i, &preLogIndex, &PrevLogTerm); //获取preLogIndex和PrevLogTerm
      std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> appendEntriesArgs =
          std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
      appendEntriesArgs->set_term(m_currentTerm);
      appendEntriesArgs->set_leaderid(m_me);
      appendEntriesArgs->set_prevlogindex(preLogIndex);
      appendEntriesArgs->set_prevlogterm(PrevLogTerm);
      appendEntriesArgs->clear_entries(); //清除 Entries 
      appendEntriesArgs->set_leadercommit(m_commitIndex); // leader节点已经提交的日志索引
      //添加Entries内容
      if (preLogIndex != m_lastSnapshotIncludeIndex) {  //m_logs不是从m_log的0索引开始的
        for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1; j < m_logs.size(); ++j) {//逻辑索引LogIndex转换成m_logs中的下标位置
          raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
          *sendEntryPtr = m_logs[j];  //=是可以点进去的，可以点进去看下protobuf如何重写这个的
        }
      } else {
        for (const auto& item : m_logs) {//m_logs从0开始
          raftRpcProctoc::LogEntry* sendEntryPtr = appendEntriesArgs->add_entries();
          *sendEntryPtr = item;  //=是可以点进去的，可以点进去看下protobuf如何重写这个的
        }
      }
      int lastLogIndex = getLastLogIndex();
      // leader对每个节点发送的日志长短不一，但是都保证从prevIndex发送直到最后
      myAssert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == lastLogIndex,
               format("appendEntriesArgs.PrevLogIndex{%d}+len(appendEntriesArgs.Entries){%d} != lastLogIndex{%d}",
                      appendEntriesArgs->prevlogindex(), appendEntriesArgs->entries_size(), lastLogIndex));
      //构造返回值appendEntriesReply
      const std::shared_ptr<raftRpcProctoc::AppendEntriesReply> appendEntriesReply =
          std::make_shared<raftRpcProctoc::AppendEntriesReply>();
      appendEntriesReply->set_appstate(Disconnected);//默认是Disconnected，如果能接收到回复就说明网络是正常的

      std::thread t(&Raft::sendAppendEntries, this, i, appendEntriesArgs, appendEntriesReply,
                    appendNums);  
      t.detach();
    }
    m_lastResetHearBeatTime = now();  // leader发送心跳时间点
  }
}


//选举超时 它会变成 Candidate，增加自己的 term，然后向其他节点请求投票  尝试成为Leader 
void Raft::electionTimeOutTicker() {  //协程，一直在运行，定时检查是否需要发起选举
 
  while (true) {
    /**
     * 如果不睡眠，那么对于leader，这个函数会一直空转，浪费cpu。且加入协程之后，
     * 空转会导致其他协程无法运行，对于时间敏感的AE，会导致心跳无法正常发送导致异常
     */
    std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};//时间长度类型，初始化为0，time=n*ratio
    std::chrono::system_clock::time_point wakeTime{};  //时间点类型，初始化为0，
    std::chrono::system_clock::time_point observedResetTime{};
    bool isLeader = false;
    {
      std::lock_guard<std::mutex> lock(m_mtx);
      isLeader = m_status == Leader;
      wakeTime = now();
      observedResetTime = m_lastResetElectionTime;
      //这里使用一个随机数来计算睡眠时间，避免多个节点同时选举导致的冲突，随机数的范围是0到ElectionTimeout之间
      suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;//距离下一次选举还剩多少时间
    }
    if (isLeader) {
      usleep(1000 * HeartBeatTimeout);
      continue;
    }
    //如果距离下一次选举还有时间，那么就睡眠，等待下一次选举的到来
    if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) { //大于1ms
      usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
    }

    doElection(observedResetTime);
  }
}

//把已经 commit、但还没有 apply 到状态机的日志取出来，封装成 ApplyMsg，返回给上层状态机去执行。
std::vector<ApplyMsg> Raft::getApplyLogs() {
  std::vector<ApplyMsg> applyMsgs;
  myAssert(m_commitIndex <= getLastLogIndex(), format("[func-getApplyLogs-rf{%d}] commitIndex{%d} >getLastLogIndex{%d}",
                                                      m_me, m_commitIndex, getLastLogIndex()));

  while (m_lastApplied < m_commitIndex) {
    m_lastApplied++;
    myAssert(m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex() == m_lastApplied,
             format("rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogIndex{%d} != rf.lastApplied{%d} ",
                    m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex(), m_lastApplied));
    ApplyMsg applyMsg;
    applyMsg.CommandValid = true;
    applyMsg.SnapshotValid = false;
    applyMsg.Command = m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].command();
    applyMsg.CommandIndex = m_lastApplied;
    applyMsgs.emplace_back(applyMsg);
   
  }
  return applyMsgs;
}

// 获取新命令应该分配的Index
int Raft::getNewCommandIndex() {
  //	如果len(logs)==0,就为快照的index+1，否则为log最后一个日志+1
  auto lastLogIndex = getLastLogIndex();
  return lastLogIndex + 1;
}

// getPrevLogInfo
// leader调用，传入：服务器index，传出：发送的AE的preLogIndex和PrevLogTerm
void Raft::getPrevLogInfo(int server, int* preIndex, int* preTerm) {
  // logs长度为0返回0,0，不是0就根据nextIndex数组的数值返回
  if (m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1) {
    //要发送的日志是第一个日志，因此直接返回m_lastSnapshotIncludeIndex和m_lastSnapshotIncludeTerm
    *preIndex = m_lastSnapshotIncludeIndex;
    *preTerm = m_lastSnapshotIncludeTerm;
    return;
  }
  auto nextIndex = m_nextIndex[server];
  *preIndex = nextIndex - 1;
  *preTerm = m_logs[getSlicesIndexFromLogIndex(*preIndex)].logterm();
}

// GetState return currentTerm and whether this server
// believes it is the Leader.
void Raft::GetState(int* term, bool* isLeader) {
  m_mtx.lock();
  DEFER {
    // todo 暂时不清楚会不会导致死锁
    m_mtx.unlock();
  };

  *term = m_currentTerm;
  *isLeader = (m_status == Leader);
}

RaftStatus Raft::GetStatusSnapshot() {
  std::lock_guard<std::mutex> lock(m_mtx);
  RaftRole role = RaftRole::Follower;
  if (m_status == Candidate) {
    role = RaftRole::Candidate;
  } else if (m_status == Leader) {
    role = RaftRole::Leader;
  }
  return {m_me, m_currentTerm, role, m_commitIndex, m_lastApplied,
          m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm};
}

//真正的安装快照函数
void Raft::InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest* args,
                           raftRpcProctoc::InstallSnapshotResponse* reply) {
  m_mtx.lock();
  DEFER { m_mtx.unlock(); };
  if (args->term() < m_currentTerm) {
    reply->set_term(m_currentTerm);

    return;
  }
  DEFER { m_lastResetElectionTime = now(); };
  if (args->term() > m_currentTerm) {
    //后面两种情况都要接收日志
    m_currentTerm = args->term();
    m_votedFor = -1;
    m_status = Follower;
    persist();
  }
  m_status = Follower;  //==时 candidate也要变成follower，防止出现两个leader的情况
  reply->set_term(m_currentTerm);

  std::vector<kvraft::LogPosition> positions;
  positions.reserve(m_logs.size());
  for (const auto& log : m_logs) {
    positions.push_back({log.logindex(), log.logterm()});
  }
  const auto plan = kvraft::PlanSnapshotInstall(
      m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm, positions,
      args->lastsnapshotincludeindex(), args->lastsnapshotincludeterm());
  if (plan.decision == kvraft::SnapshotDecision::Stale ||
      plan.decision == kvraft::SnapshotDecision::Idempotent) {
    return;
  }
  if (plan.decision == kvraft::SnapshotDecision::Conflict) {
    throw kvraft::PersistenceError("snapshot term conflicts at installed index");
  }

  m_logs.erase(m_logs.begin(), m_logs.begin() + plan.firstRetainedLog);
  m_lastSnapshotIncludeIndex = args->lastsnapshotincludeindex();
  m_lastSnapshotIncludeTerm = args->lastsnapshotincludeterm();
  m_persister->Save(persistData(), args->data());
  m_commitIndex = std::max(m_commitIndex, args->lastsnapshotincludeindex());
  m_lastApplied = std::max(m_lastApplied, args->lastsnapshotincludeindex());

  ApplyMsg msg; 
  msg.SnapshotValid = true;
  msg.Snapshot = args->data(); //快照
  msg.SnapshotTerm = args->lastsnapshotincludeterm();
  msg.SnapshotIndex = args->lastsnapshotincludeindex();
  applyChan->Push(msg);
}

void Raft::pushMsgToKvServer(ApplyMsg msg) { applyChan->Push(msg); }

void Raft::leaderHearBeatTicker() {
  while (true) {
    std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
    std::chrono::system_clock::time_point wakeTime{};
    std::chrono::system_clock::time_point observedResetTime{};
    bool isLeader = false;
    {
      std::lock_guard<std::mutex> lock(m_mtx);
      isLeader = m_status == Leader;
      wakeTime = now();
      observedResetTime = m_lastResetHearBeatTime;
      suitableSleepTime = std::chrono::milliseconds(HeartBeatTimeout) + m_lastResetHearBeatTime - wakeTime;
    }
    if (!isLeader) {
      usleep(1000 * HeartBeatTimeout);
      continue;
    }

    if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
      usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
    }

    {
      std::lock_guard<std::mutex> lock(m_mtx);
      if (m_status != Leader || m_lastResetHearBeatTime != observedResetTime) continue;
    }
    doHeartBeat();
  }
}

//leader 发现某个 follower 的 nextIndex 已经落到快照之前，
//就发送 InstallSnapshot，成功后更新该 follower 的 matchIndex / nextIndex
void Raft::leaderSendSnapShot(int server) {
  m_mtx.lock();
  raftRpcProctoc::InstallSnapshotRequest args; 
  args.set_leaderid(m_me);
  args.set_term(m_currentTerm);
  args.set_lastsnapshotincludeindex(m_lastSnapshotIncludeIndex);
  args.set_lastsnapshotincludeterm(m_lastSnapshotIncludeTerm);
  args.set_data(m_persister->ReadSnapshot());  //快照信息

  raftRpcProctoc::InstallSnapshotResponse reply;  // term
  m_mtx.unlock();
  //RPC可能会很慢，RPC之前先解锁，PRC阻塞等待
  bool ok = m_peers[server]->InstallSnapshot(&args, &reply);//目标服务器安装快照，成功返回true，失败返回false
  m_mtx.lock();    ///
  DEFER { m_mtx.unlock(); };
  if (!ok) {
    return;
  }
  if (m_status != Leader || m_currentTerm != args.term()) {
    return;  //中间释放过锁，可能状态已经改变了
  }
  //	无论什么时候都要判断term
  if (reply.term() > m_currentTerm) {
    //三变
    m_currentTerm = reply.term();
    m_votedFor = -1;
    m_status = Follower;
    persist();
    m_lastResetElectionTime = now();
    return;
  }
  m_matchIndex[server] = args.lastsnapshotincludeindex();
  m_nextIndex[server] = m_matchIndex[server] + 1;
}

//leader更新commitIndex，只有当前term有新提交的，才会更新commitIndex
void Raft::leaderUpdateCommitIndex() {
  m_commitIndex = m_lastSnapshotIncludeIndex;
  for (int index = getLastLogIndex(); index >= m_lastSnapshotIncludeIndex + 1; index--) {
    int sum = 0;
    for (int i = 0; i < m_peers.size(); i++) {
      if (i == m_me) {
        sum += 1;
        continue;
      }
      if (m_matchIndex[i] >= index) {
        sum += 1;
      }
    }

    //        !!!只有当前term有新提交的，才会更新commitIndex！！！！
    if (sum >= m_peers.size() / 2 + 1 && getLogTermFromLogIndex(index) == m_currentTerm) {
      m_commitIndex = index;
      break;
    }
  }
}

//判断logIndex和logTerm是否匹配，匹配的定义是：logIndex存在，并且其term等于传入的logTerm
bool Raft::matchLog(int logIndex, int logTerm) {
  myAssert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(),
           format("不满足：logIndex{%d}>=rf.lastSnapshotIncludeIndex{%d}&&logIndex{%d}<=rf.getLastLogIndex{%d}",
                  logIndex, m_lastSnapshotIncludeIndex, logIndex, getLastLogIndex()));
  return logTerm == getLogTermFromLogIndex(logIndex);
  
}


void Raft::persist() {
  auto data = persistData();
  m_persister->SaveRaftState(data);
}


//重载Raft::RequestVote
void Raft::RequestVote(const raftRpcProctoc::RequestVoteArgs* args, raftRpcProctoc::RequestVoteReply* reply) {
  std::lock_guard<std::mutex> lg(m_mtx);  //保护自身raft节点状态的线程安全，涉及到m_status，m_currentTerm，m_votedFor等

  bool persistentStateChanged = false;
  bool resetElectionTimer = false;
  DEFER {  //宏定义，用类的析构来管理资源 DEFER在离开作用域的时候自动调用析构函数，然后调用persist
    //离开作用域时 ，应该先持久化，再unlock，  DEFER写在lg后面
    if (persistentStateChanged) persist();
    if (resetElectionTimer) m_lastResetElectionTime = now();
  };

  //对args的term的三种情况分别进行处理，大于小于等于自己的term都是不同的处理
  // reason: 出现网络分区，该竞选者已经OutOfDate(过时）
  if (args->term() < m_currentTerm) { // 1.请求的term过时了
    reply->set_term(m_currentTerm);
    reply->set_votestate(Expire);  //过时
    reply->set_votegranted(false);
    return;
  }
  // fig2:右下角，如果任何时候rpc请求或者响应的term大于自己的term，更新term，并变成follower
  if (args->term() > m_currentTerm) {
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;
    persistentStateChanged = true;

    //	重置定时器：收到leader的ae，开始选举，投出票
    //这时候更新了term之后，votedFor也要置为-1
  }
  myAssert(args->term() == m_currentTerm,format("[func--rf{%d}] 前面校验过args.Term==rf.currentTerm，这里却不等", m_me));
  int lastLogTerm = getLastLogTerm();
   /*	现在节点任期都是相同的(任期小的也已经更新到新的args的term了)，还需要检查log的term和index是不是匹配的了
    只有当还没投票，且candidate的日志的新的程度 ≥ 接受者的日志新的程度 才会投票*/

  //请求日志比当前投票者的日志还要旧，拒绝投票
  if (!UpToDate(args->lastlogindex(), args->lastlogterm())) { 
    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);  //已经投过票了，拒绝投票
    reply->set_votegranted(false);
    return;
  }
  
  // 当因为网络质量不好导致的请求丢失重发就有可能，这里只能投一票
  if (m_votedFor != -1 && m_votedFor != args->candidateid()) {  //已经投过票了，并且投的不是这个candidate，拒绝投票
    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);
    reply->set_votegranted(false);
    return;
  } 
  else { //没有投票，或者投的就是这个candidate，那么就投票，并且重置定时器
    persistentStateChanged = persistentStateChanged || m_votedFor != args->candidateid();
    m_votedFor = args->candidateid(); 
    resetElectionTimer = true;
    reply->set_term(m_currentTerm);
    reply->set_votestate(Normal);
    reply->set_votegranted(true);
    return;
  }
}

//比较candidate的日志和自己的日志哪个更新
bool Raft::UpToDate(int index, int term) {  
                                            //更新的定义是：term更大，或者term相同但是index更大
  // lastEntry := rf.log[len(rf.log)-1]

  int lastIndex = -1;
  int lastTerm = -1;
  getLastLogIndexAndTerm(&lastIndex, &lastTerm);
  return term > lastTerm || (term == lastTerm && index >= lastIndex);
}

//获取最后一个日志的index和term，从日志或快照中获取
void Raft::getLastLogIndexAndTerm(int* lastLogIndex, int* lastLogTerm) {
  if (m_logs.empty()) {  //日志为空则说明没有日志了，返回快照的index和term
    *lastLogIndex = m_lastSnapshotIncludeIndex;
    *lastLogTerm = m_lastSnapshotIncludeTerm;
    return;
  } else {//日志不为空则返回最后一个日志的index和term
    *lastLogIndex = m_logs[m_logs.size() - 1].logindex();
    *lastLogTerm = m_logs[m_logs.size() - 1].logterm();
    return;
  }
}

int Raft::getLastLogIndex() {
  int lastLogIndex = -1;
  int _ = -1;
  getLastLogIndexAndTerm(&lastLogIndex, &_);
  return lastLogIndex;
}

int Raft::getLastLogTerm() {
  int _ = -1;
  int lastLogTerm = -1;
  getLastLogIndexAndTerm(&_, &lastLogTerm);
  return lastLogTerm;
}

//获取logIndex对应的日志的term
int Raft::getLogTermFromLogIndex(int logIndex) { 
  myAssert(logIndex >= m_lastSnapshotIncludeIndex,
           format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} < rf.lastSnapshotIncludeIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));

  int lastLogIndex = getLastLogIndex();

  myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                            m_me, logIndex, lastLogIndex));

  if (logIndex == m_lastSnapshotIncludeIndex) {
    return m_lastSnapshotIncludeTerm;
  } else {
    return m_logs[getSlicesIndexFromLogIndex(logIndex)].logterm();
  }
}


int Raft::GetRaftStateSize() { return m_persister->RaftStateSize(); }  //获取raftstate的大小


//截取日志保存为快照时，m_logs(vector)会从0开始存储快照之后的日志，因此需要一个函数来将logIndex转换为m_logs中的下标位置
// 限制，输入的logIndex必须保存在当前的logs里面（不包含snapshot）
int Raft::getSlicesIndexFromLogIndex(int logIndex) {
  myAssert(logIndex > m_lastSnapshotIncludeIndex,
           format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} <= rf.lastSnapshotIncludeIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));
  int lastLogIndex = getLastLogIndex();
  myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                            m_me, logIndex, lastLogIndex));

  int SliceIndex = logIndex - m_lastSnapshotIncludeIndex - 1;//
  return SliceIndex;  //m_logs中的下标位置
}


//这个函数发送RequestVote RPC，并处理回复，只简单判断term，日志等其他判断在RequestVote函数
bool Raft::sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                           std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum) {
 
  auto start = std::chrono::steady_clock::now();
  DPrintf("[func-sendRequestVote rf{%d}] 向server{%d}发送term{%d}的RequestVote开始", m_me, server, args->term());
  bool ok = m_peers[server]->RequestVote(args.get(), reply.get());  //这个函数是同步的，直到收到回复才会返回，
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start).count();
  DPrintf("[func-sendRequestVote rf{%d}] 向server{%d}发送term{%d}的RequestVote完成，耗时{%lld}ms", m_me, server,
          args->term(), static_cast<long long>(elapsed));

  if (!ok) {
    return ok;  //网络不通，或者对方节点宕机了，反正就是没有收到回复了，那么就没有必要进行后续处理了
  }
  //这里是发送出去了，但是不能保证他一定到达
  //对回应进行处理，要记得无论什么时候收到回复就要检查term
  std::lock_guard<std::mutex> lg(m_mtx);
  if (reply->term() > m_currentTerm) {  //如果收到的回复的term比自己大，那么就说明自己已经过时了，要变成follower了
    m_status = Follower;  //三变：身份，term，和投票
    m_currentTerm = reply->term();
    m_votedFor = -1;
    persist();
    m_lastResetElectionTime = now();
    return true;
  } else if (reply->term() < m_currentTerm) { //如果收到的回复的term比自己小，那么就说明对方已经过时了，直接忽略这个回复就好了，不用进行后续处理了
    return true;
  }
  myAssert(reply->term() == m_currentTerm, format("assert {reply.Term==rf.currentTerm} fail"));//前面已经校验过了，这里应该是相等的了，如果不相等就说明代码哪里写错了

  if (m_status != Candidate || args->term() != m_currentTerm) {
    return true;
  }
  if (!reply->votegranted()) {
    return true;
  }

  *votedNum = *votedNum + 1;
  if (*votedNum >= m_peers.size() / 2 + 1) {
    //变成leader
    *votedNum = 0;
    if (m_status == Leader) {
      //如果已经是leader了，不会进行下一步处理了
      myAssert(false,format("[func-sendRequestVote-rf{%d}]  term:{%d} 同一个term当两次领导，error", m_me, m_currentTerm));
    }
    //	第一次变成leader，初始化状态和nextIndex、matchIndex
    m_status = Leader;

    DPrintf("[func-sendRequestVote rf{%d}] elect success  ,current term:{%d} ,lastLogIndex:{%d}\n", m_me, m_currentTerm,
            getLastLogIndex());

    int lastLogIndex = getLastLogIndex(); //获取追加no-op前的最后一个日志index
    for (int i = 0; i < m_nextIndex.size(); i++) {
      m_nextIndex[i] = lastLogIndex + 1;  //下一次要发送给节点 i 的日志下标
      m_matchIndex[i] = 0;                //每换一个领导都是从0开始，见fig2
    }

    Op noOp;
    noOp.Operation = "NoOp";
    noOp.Key.clear();
    noOp.Value.clear();
    noOp.ClientId = "raft-internal";
    noOp.RequestId = m_currentTerm;
    raftRpcProctoc::LogEntry noOpEntry;
    noOpEntry.set_command(noOp.asString());
    noOpEntry.set_logterm(m_currentTerm);
    noOpEntry.set_logindex(lastLogIndex + 1);
    m_logs.emplace_back(noOpEntry);
    persist();

    std::thread t(&Raft::doHeartBeat, this);  //马上向其他节点宣告自己就是leader
    t.detach();
  }
  return true;
}


//发送AppendEntries RPC，并处理回复  doHeartBeat函数调用新的线程执行
bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                             std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply,
                             std::shared_ptr<int> appendNums) {

  //这个ok是网络是否正常通信的ok，如果网络不通的话肯定是没有返回的，不用一直重试
  DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc开始 ， args->entries_size():{%d}", m_me,
          server, args->entries_size());
  bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());//调用目标节点 server 上的 AppendEntries RPC 处理函数。
  //输出日志 ，查看leader发送AE 情况
  if (!ok) {
    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc失败", m_me, server);
    return ok;
  }
  DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc成功", m_me, server);
  if (reply->appstate() == Disconnected) { //业务层面是否正常返回 （冗余）
    return ok;  //
  }

  //对reply进行处理
  std::lock_guard<std::mutex> lg1(m_mtx);
  // 对于rpc通信，无论什么时候都要检查term
  if (reply->term() > m_currentTerm) {
    m_status = Follower;  //
    m_currentTerm = reply->term();
    m_votedFor = -1;
    persist();
    m_lastResetElectionTime = now();
    return ok;
  } else if (reply->term() < m_currentTerm) {
    DPrintf("[func -sendAppendEntries  rf{%d}]  节点：{%d}的term{%d}<rf{%d}的term{%d}\n", m_me, server, reply->term(),
            m_me, m_currentTerm);
    return ok;
  }

  if (m_status != Leader || args->term() != m_currentTerm) {
    //如果已经不是发起该请求的leader，那么就不要处理陈旧响应
    return ok;
  }

  // term相等
  myAssert(reply->term() == m_currentTerm,
           format("reply.Term{%d} != rf.currentTerm{%d}   ", reply->term(), m_currentTerm));
  if (!reply->success()) {
    //日志不匹配，回退nextIndex
    if (reply->updatenextindex() != -100) {  //有效的返回，说明是日志不匹配导致的失败，而不是网络不通导致的没有返回
      // todo:待总结修改，就算term匹配，失败的时候nextIndex也不是照单全收的，因为如果发生rpc延迟，leader的term可能从不符合term要求
      //变得符合term要求(rpc ae并行发送，不是串行的话会出现先发的rpc后返回，此时会导致乱序)
      //但是不能直接赋值m_nextIndex[server] =reply.UpdateNextIndex();
      DPrintf("[func -sendAppendEntries  rf{%d}]  返回的日志term相等，但是不匹配，回缩nextIndex[%d]：{%d}\n", m_me,
              server, reply->updatenextindex());
      m_nextIndex[server] = reply->updatenextindex();  //失败更新nextIndex，不更新mathIndex
    }
  } 
  else {
    *appendNums = *appendNums + 1;
    DPrintf("---------------------------tmp------------------------- 节点{%d}返回true,当前*appendNums{%d}", server,
            *appendNums);

    // rf.matchIndex[server] = len(args.Entries) //只要返回一个响应就对其matchIndex应该对其做出反应，
    //但是这么修改是有问题的，如果对某个消息发送了多遍（reply未返回时，心跳时就会再发送entries），那么一条消息会导致n次上涨
    m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries_size());
    m_nextIndex[server] = m_matchIndex[server] + 1;
    int lastLogIndex = getLastLogIndex();

    myAssert(m_nextIndex[server] <= lastLogIndex + 1,
             format("error msg:rf.nextIndex[%d] > lastLogIndex+1, len(rf.logs) = %d   lastLogIndex{%d} = %d", server,
                    m_logs.size(), server, lastLogIndex));
    if (*appendNums >= 1 + m_peers.size() / 2) { 
      //可以commit了
      //两种方法保证幂等性，1.appendNums赋值为0 	2.appendNums >= 1 + m_peers.size() / 2   ≥改为==  防止重复提交

      *appendNums = 0;
      // todo https://578223592-laughing-halibut-wxvpggvw69qh99q4.github.dev/ 不断遍历来统计rf.commitIndex
      // leader只有在当前term有日志提交的时候才更新commitIndex，因为raft无法保证之前term的Index是否提交
      //只有当前term有日志提交，之前term的log才可以被顺带提交，防止当前term日志覆盖之前term提交的日志，造成之前term提交的日志丢失了
      //只有这样才能保证“领导人完备性{当选领导人的节点拥有之前被提交的所有log，当然也可能有一些没有被提交的}”
      // rf.leaderUpdateCommitIndex()
      if (args->entries_size() > 0) {
        DPrintf("args->entries(args->entries_size()-1).logterm(){%d}   m_currentTerm{%d}",
                args->entries(args->entries_size() - 1).logterm(), m_currentTerm);
      }
      //AppendEntries 真的发送了日志，并且最后一条日志的term是当前term，才会更新commitIndex
      if (args->entries_size() > 0 && args->entries(args->entries_size() - 1).logterm() == m_currentTerm) {
        DPrintf(
            "---------------------------tmp------------------------- 当前term有log成功提交，更新leader的m_commitIndex "
            "from{%d} to{%d}",
            m_commitIndex, args->prevlogindex() + args->entries_size());

        m_commitIndex = std::max(m_commitIndex, args->prevlogindex() + args->entries_size());
      }
      myAssert(m_commitIndex <= lastLogIndex,
               format("[func-sendAppendEntries,rf{%d}] lastLogIndex:%d  rf.commitIndex:%d\n", m_me, lastLogIndex,
                      m_commitIndex));
    }
  }
  return ok;
}


//RPC入口函数 follower和candidate的AppendEntries和RequestVote函数，leader的InstallSnapshot函数
void Raft::AppendEntries(google::protobuf::RpcController* controller,
                         const ::raftRpcProctoc::AppendEntriesArgs* request,
                         ::raftRpcProctoc::AppendEntriesReply* response, ::google::protobuf::Closure* done) {
  AppendEntries1(request, response);  //Raft::AppendEntries1  调用重载的AppendEntries1函数  
  done->Run();
}

void Raft::InstallSnapshot(google::protobuf::RpcController* controller,
                           const ::raftRpcProctoc::InstallSnapshotRequest* request,
                           ::raftRpcProctoc::InstallSnapshotResponse* response, ::google::protobuf::Closure* done) {
  InstallSnapshot(request, response);

  done->Run();
}

void Raft::RequestVote(google::protobuf::RpcController* controller, const ::raftRpcProctoc::RequestVoteArgs* request,
                       ::raftRpcProctoc::RequestVoteReply* response, ::google::protobuf::Closure* done) {
  RequestVote(request, response); //Raft::RequestVote  调用重载的RequestVote函数  //640行
  done->Run();
}

//上层服务（kvserver）调用Start来提交一个新的命令，Start会把这个命令追加到**leader**的日志中，
//并返回这个命令在日志中的index和term，以及当前节点是否是leader

//`Raft::Start` `Raft::doHeartBeat`  `Raft::getPrevLogInfo`  `Raft::AppendEntries1` 
// `Raft::sendAppendEntries` `Raft::applierTicker` 和 `Raft::getApplyLogs`
void Raft::Start(Op command, int* newLogIndex, int* newLogTerm, bool* isLeader) {
  std::lock_guard<std::mutex> lg1(m_mtx);
  if (m_status != Leader) {
    DPrintf("[func-Start-rf{%d}]  is not leader");
    *newLogIndex = -1;
    *newLogTerm = -1;
    *isLeader = false;
    return;
  }

  raftRpcProctoc::LogEntry newLogEntry;
  newLogEntry.set_command(command.asString()); //序列化命令，方便网络传输
  newLogEntry.set_logterm(m_currentTerm);      //日志的term就是当前leader的term
  newLogEntry.set_logindex(getNewCommandIndex());  //获取新命令的index
  m_logs.emplace_back(newLogEntry);

  int lastLogIndex = getLastLogIndex();   //获取当前leader的最后一个日志的index，这里的日志指的是客户端的请求日志

  // leader应该不停的向各个Follower发送AE来维护心跳和保持日志同步，目前的做法是新的命令来了不会直接执行，而是等待leader的心跳触发
  DPrintf("[func-Start-rf{%d}]  lastLogIndex:%d,command:%s\n", m_me, lastLogIndex, &command);
  // rf.timer.Reset(10) //接收到命令后马上给follower发送,改成这样不知为何会出现问题，待修正 todo
  persist();
  *newLogIndex = newLogEntry.logindex();
  *newLogTerm = newLogEntry.logterm();
  *isLeader = true;
}

//保存依赖、初始化状态并恢复崩溃前持久化数据。后台任务由 StartBackgroundTasks 启动。
void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh) {  //ApplyMsg：Raft提交给上层状态机的消息
  m_peers = peers;   //RaftRpcUtil把Rpc通信细节封装起来，让 Raft 类可以简单调用。
  m_persister = persister;  //持久化
  m_me = me;
  // Your initialization code here (2A, 2B, 2C).
  m_mtx.lock();

  // applier
  this->applyChan = applyCh;
  //    rf.ApplyMsgQueue = make(chan ApplyMsg)
  m_currentTerm = 0;
  m_status = Follower;
  m_commitIndex = 0;
  m_lastApplied = 0;
  m_logs.clear();
  for (int i = 0; i < m_peers.size(); i++) {
    m_matchIndex.push_back(0);
    m_nextIndex.push_back(0);
  }
  m_votedFor = -1;

  m_lastSnapshotIncludeIndex = 0;
  m_lastSnapshotIncludeTerm = 0;
  m_lastResetElectionTime = now();
  m_lastResetHearBeatTime = now();

  // initialize from state persisted before a crash 从崩溃前持久化保存的状态中恢复初始化
  readPersist(m_persister->ReadRaftState());
  m_commitIndex = m_lastSnapshotIncludeIndex;
  m_lastApplied = m_lastSnapshotIncludeIndex;
  //
  DPrintf("[Init&ReInit] Server %d, term %d, lastSnapshotIncludeIndex {%d} , lastSnapshotIncludeTerm {%d}", m_me,
          m_currentTerm, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);

  m_mtx.unlock();
}

void Raft::StartBackgroundTasks() {
  {
    std::lock_guard<std::mutex> lock(m_mtx);
    if (m_backgroundTasksStarted) return;
    m_backgroundTasksStarted = true;
  }
  m_ioManager = std::make_unique<monsoon::IOManager>(FIBER_THREAD_NUM, FIBER_USE_CALLER_THREAD);
  //IO管理器的作用是管理协程的调度和执行，FIBER_THREAD_NUM：协程库中线程池大小 FIBER_USE_CALLER_THREAD 是否使用caller_thread执行调度任务

  // start ticker fiber to start elections
  // 启动三个循环定时器

  // 、electionTimeOutTicker执行时间是恒定的，applierTicker时间受到数据库响应延迟和两次apply之间请求数量
  //的影响，这个随着数据量增多可能不太合理，最好其还是启用一个线程。
  m_ioManager->scheduler([this]() -> void { this->leaderHearBeatTicker(); });  //leader心跳
  m_ioManager->scheduler([this]() -> void { this->electionTimeOutTicker(); }); //选举超时

  std::thread t3(&Raft::applierTicker, this); //日志应用  这里可以会阻塞，单开线程
  t3.detach();
}
//将raft节点的状态持久化到磁盘中，返回序列化后的字符串 
std::string Raft::persistData() {
  BoostPersistRaftNode boostPersistRaftNode;
  boostPersistRaftNode.m_currentTerm = m_currentTerm;
  boostPersistRaftNode.m_votedFor = m_votedFor;
  boostPersistRaftNode.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
  boostPersistRaftNode.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;
  for (auto& item : m_logs) {
    boostPersistRaftNode.m_logs.push_back(item.SerializeAsString());//对日志进行序列化（protobuf），方便网络传输
  }

  std::stringstream ss;
  boost::archive::text_oarchive oa(ss);
  oa << boostPersistRaftNode; //对整个raft节点的状态进行序列化，方便持久化保存
  return ss.str();
}

void Raft::readPersist(std::string data) {
  if (data.empty()) {
    return;
  }
  std::stringstream iss(data);
  boost::archive::text_iarchive ia(iss);
  // read class state from archive
  BoostPersistRaftNode boostPersistRaftNode;
  ia >> boostPersistRaftNode;

  m_currentTerm = boostPersistRaftNode.m_currentTerm;
  m_votedFor = boostPersistRaftNode.m_votedFor;
  m_lastSnapshotIncludeIndex = boostPersistRaftNode.m_lastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = boostPersistRaftNode.m_lastSnapshotIncludeTerm;
  m_logs.clear();
  for (auto& item : boostPersistRaftNode.m_logs) {
    raftRpcProctoc::LogEntry logEntry;
    logEntry.ParseFromString(item);
    m_logs.emplace_back(logEntry);
  }
}


//当快照完成后（KV层），更新raft节点的状态，保存快照，并删除已经被快照包含的日志
void Raft::Snapshot(int index, std::string snapshot) {
  std::lock_guard<std::mutex> lg(m_mtx);

  if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex) {//快照已经过期或者快照包含的日志还没有提交，
    DPrintf(
        "[func-Snapshot-rf{%d}] rejects replacing log with snapshotIndex %d as current snapshotIndex %d is larger or "
        "smaller ",
        m_me, index, m_lastSnapshotIncludeIndex);
    return;
  }
  auto lastLogIndex = getLastLogIndex();  //为了检查snapshot前后日志是否一样，防止多截取或者少截取日志

  //制造完此快照后剩余的所有日志
  int newLastSnapshotIncludeIndex = index;
  int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
  std::vector<raftRpcProctoc::LogEntry> trunckedLogs;
  // todo :这种写法有点笨，待改进，而且有内存泄漏的风险
  for (int i = index + 1; i <= getLastLogIndex(); i++) {
    //注意有=，因为要拿到最后一个日志
    trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
  }
  m_lastSnapshotIncludeIndex = newLastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;
  m_logs = trunckedLogs;//丢弃index之前的日志，保留index之后的日志
  m_commitIndex = std::max(m_commitIndex, index);
  m_lastApplied = std::max(m_lastApplied, index);
  m_persister->Save(persistData(), snapshot);//保存持久化状态和快照数据

  DPrintf("[SnapShot]Server %d snapshot snapshot index {%d}, term {%d}, loglen {%d}", m_me, index,
          m_lastSnapshotIncludeTerm, m_logs.size());
  myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
           format("len(rf.logs){%d} + rf.lastSnapshotIncludeIndex{%d} != lastLogjInde{%d}", m_logs.size(),
                  m_lastSnapshotIncludeIndex, lastLogIndex));
}
