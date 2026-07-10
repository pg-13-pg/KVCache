
#ifndef SKIP_LIST_ON_RAFT_PERSISTER_H  //头文件保护宏可以防止同一个头文件在同一个编译单元里被重复展开。
#define SKIP_LIST_ON_RAFT_PERSISTER_H
#include <fstream>
#include <mutex>
class Persister {
 private:
  std::mutex m_mtx;
  std::string m_raftState;  
  std::string m_snapshot;  
 // m_raftStateFileName: raftState文件名
  const std::string m_raftStateFileName;
 // m_snapshotFileName: snapshot文件名
  const std::string m_snapshotFileName;
  //保存raftState的输出流
  std::ofstream m_raftStateOutStream;
//保存snapshot的输出流
  std::ofstream m_snapshotOutStream;
  // 保存raftStateSize的大小，避免每次都读取文件来获取具体的大小
  long long m_raftStateSize;

 public:
  void Save(std::string raftstate, std::string snapshot); //保存raftstate和snapshot到本地文件
  std::string ReadSnapshot();  //从本地文件读取snapshot
  void SaveRaftState(const std::string& data);  //保存raftstate到本地文件
  long long RaftStateSize();  //获取raftstate的大小
  std::string ReadRaftState(); //从本地文件读取raftstate
  explicit Persister(int me);  //构造函数，传入raft节点的编号，初始化文件名和输出流
  ~Persister();

 private:
  void clearRaftState();  
  void clearSnapshot();
  void clearRaftStateAndSnapshot(); 
};

#endif  // SKIP_LIST_ON_RAFT_PERSISTER_H
