
#ifndef SKIP_LIST_ON_RAFT_PERSISTER_H
#define SKIP_LIST_ON_RAFT_PERSISTER_H

#include "wal_format.h"

#include <cstddef>
#include <filesystem>
#include <mutex>
#include <string>

class Persister {
 public:
  explicit Persister(std::filesystem::path dataDir);
  explicit Persister(int me);
  ~Persister();

  void Save(std::string raftState, std::string snapshot);
  void SaveRaftState(const std::string& data);
  std::string ReadSnapshot();
  std::string ReadRaftState();
  long long RaftStateSize();
  const std::filesystem::path& WalPath() const noexcept;

 private:
  void Recover();
  void AppendRecord(kvraft::wal::RecordType type, const std::string& raftState,
                    const std::string& snapshot);
  void CompactSnapshot(const std::string& raftState, const std::string& snapshot);

  std::filesystem::path dataDir_;
  std::filesystem::path walPath_;
  int walFd_ = -1;
  std::uint64_t nextSequence_ = 1;
  std::size_t walBytes_ = 0;
  std::string raftState_;
  std::string snapshot_;
  std::mutex mutex_;
};

#endif  // SKIP_LIST_ON_RAFT_PERSISTER_H
