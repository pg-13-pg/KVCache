#include "Persister.h"
#include "wal_format.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

namespace fs = std::filesystem;

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

class TemporaryDirectory {
 public:
  TemporaryDirectory()
      : path_(fs::temp_directory_path() /
              ("kvraft-persister-wal-" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()))) {
    fs::create_directories(path_);
  }

  ~TemporaryDirectory() { fs::remove_all(path_); }

  const fs::path& path() const { return path_; }

 private:
  fs::path path_;
};

void AppendBytes(const fs::path& path, std::string_view bytes) {
  std::ofstream output(path, std::ios::binary | std::ios::app);
  Expect(output.good(), "failed to open WAL for test append");
  output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  Expect(output.good(), "failed to append WAL test data");
}

void TestBinaryRoundTrip(const fs::path& root) {
  const std::string state("term 7\nlog\0entry", 16);
  {
    Persister p(root / "binary");
    p.SaveRaftState(state);
    Expect(p.ReadRaftState() == state, "binary state changed before reopen");
  }
  Persister reopened(root / "binary");
  Expect(reopened.ReadRaftState() == state, "binary state changed after reopen");
  Expect(reopened.RaftStateSize() == static_cast<long long>(state.size()),
         "state size must describe current payload");
}

void TestSnapshotThenState(const fs::path& root) {
  Persister p(root / "snapshot");
  p.Save("raft-at-snapshot", std::string("kv\0snapshot", 11));
  p.SaveRaftState("raft-after-snapshot");
  Persister reopened(root / "snapshot");
  Expect(reopened.ReadRaftState() == "raft-after-snapshot", "latest state missing");
  Expect(reopened.ReadSnapshot() == std::string("kv\0snapshot", 11), "snapshot missing");
}

void TestStateWalCompactionPreservesSnapshot(const fs::path& root) {
  const fs::path dataDir = root / "state-compaction";
  const std::string snapshot("snapshot\0payload", 16);
  std::string latestState;
  {
    Persister p(dataDir);
    p.Save("state-at-snapshot", snapshot);
    for (int index = 0; index < 80; ++index) {
      latestState.assign(16 * 1024, static_cast<char>('a' + index % 26));
      latestState.replace(0, 8, std::to_string(index));
      p.SaveRaftState(latestState);
    }
    Expect(fs::file_size(p.WalPath()) <= 1024 * 1024,
           "state-only WAL grew beyond its compaction bound");
  }

  Persister reopened(dataDir);
  Expect(reopened.ReadRaftState() == latestState, "compaction lost the latest Raft state");
  Expect(reopened.ReadSnapshot() == snapshot, "state compaction lost the installed snapshot");
}

void TestPartialHeaderTail(const fs::path& root) {
  fs::path wal;
  {
    Persister p(root / "partial-header");
    p.SaveRaftState("durable-state");
    wal = p.WalPath();
  }
  AppendBytes(wal, "tail");

  Persister reopened(root / "partial-header");
  Expect(reopened.ReadRaftState() == "durable-state", "partial header changed prior state");
  Expect(fs::file_size(wal) == kvraft::wal::Encode(
                                  {kvraft::wal::RecordType::State, 1, "durable-state", ""})
                                  .size(),
         "partial header tail was not truncated");
}

void TestPartialPayloadTail(const fs::path& root) {
  fs::path wal;
  {
    Persister p(root / "partial-payload");
    p.SaveRaftState("durable-state");
    wal = p.WalPath();
  }
  const std::string incomplete =
      kvraft::wal::Encode({kvraft::wal::RecordType::State, 2, "incomplete-payload", ""});
  AppendBytes(wal, std::string_view(incomplete).substr(0, kvraft::wal::kHeaderSize + 2));

  Persister reopened(root / "partial-payload");
  Expect(reopened.ReadRaftState() == "durable-state", "partial payload changed prior state");
  Expect(fs::file_size(wal) == kvraft::wal::Encode(
                                  {kvraft::wal::RecordType::State, 1, "durable-state", ""})
                                  .size(),
         "partial payload tail was not truncated");
}

void TestCompleteChecksumCorruption(const fs::path& root) {
  fs::path wal;
  {
    Persister p(root / "checksum");
    p.SaveRaftState("durable-state");
    wal = p.WalPath();
  }
  std::fstream file(wal, std::ios::binary | std::ios::in | std::ios::out);
  Expect(file.good(), "failed to open WAL for corruption test");
  file.seekp(static_cast<std::streamoff>(kvraft::wal::kHeaderSize));
  file.put('X');
  file.close();

  bool rejected = false;
  try {
    Persister reopened(root / "checksum");
  } catch (const kvraft::PersistenceError&) {
    rejected = true;
  }
  Expect(rejected, "complete checksum corruption was accepted");
}

void TestNonMonotonicSequence(const fs::path& root) {
  fs::path wal;
  {
    Persister p(root / "sequence");
    p.SaveRaftState("first-state");
    wal = p.WalPath();
  }
  AppendBytes(wal, kvraft::wal::Encode({kvraft::wal::RecordType::State, 1, "second-state", ""}));

  bool rejected = false;
  try {
    Persister reopened(root / "sequence");
  } catch (const kvraft::PersistenceError&) {
    rejected = true;
  }
  Expect(rejected, "non-monotonic sequence was accepted");
}

void TestIndependentDirectories(const fs::path& root) {
  {
    Persister node0(root / "node-0");
    Persister node1(root / "node-1");
    node0.SaveRaftState("node-zero-state");
    node1.SaveRaftState("node-one-state");
  }
  Persister node0(root / "node-0");
  Persister node1(root / "node-1");
  Expect(node0.ReadRaftState() == "node-zero-state", "node-0 state leaked or was lost");
  Expect(node1.ReadRaftState() == "node-one-state", "node-1 state leaked or was lost");
}

}  // namespace

int main() {
  try {
    TemporaryDirectory directory;
    TestBinaryRoundTrip(directory.path());
    TestSnapshotThenState(directory.path());
    TestStateWalCompactionPreservesSnapshot(directory.path());
    TestPartialHeaderTail(directory.path());
    TestPartialPayloadTail(directory.path());
    TestCompleteChecksumCorruption(directory.path());
    TestNonMonotonicSequence(directory.path());
    TestIndependentDirectories(directory.path());
  } catch (const std::exception& error) {
    std::cerr << "persister WAL test failed: " << error.what() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
