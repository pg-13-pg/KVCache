#ifndef APPLYMSG_H
#define APPLYMSG_H
#include <string>
//Raft 往上层状态机/KVServer 交付结果用的消息结构，提交日志或快照   是 Raft 提交后通知 KVServer 的消息
class ApplyMsg {
 public:
  bool CommandValid;//表示这条消息是普通命令日志
  std::string Command;//序列化后的业务命令，比如 Put/Append/Get
  int CommandIndex;//这条日志在 Raft 中的 index
  bool SnapshotValid;
  std::string Snapshot;
  int SnapshotTerm;
  int SnapshotIndex;

 public:
  //两个valid最开始要赋予false！！
  ApplyMsg()
      : CommandValid(false),
        Command(),
        CommandIndex(-1),
        SnapshotValid(false),
        SnapshotTerm(-1),
        SnapshotIndex(-1){

        };
};

#endif  // APPLYMSG_H