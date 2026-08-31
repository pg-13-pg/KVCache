# Raft Correctness Baseline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a repeatable three-node Raft correctness baseline that survives leader failure, full process restart, WAL tail damage, log compaction, and snapshot catchup without losing acknowledged writes.

**Architecture:** Run one C++ `raftNode` process per node, control the processes with a Python standard-library CTest harness, and inspect nodes through a read-only status RPC. Replace the truncating text persister with a checksummed append-only WAL, make snapshot state atomic with Raft metadata, and use bounded `kvctl` operations for black-box assertions.

**Tech Stack:** C++20, CMake 3.22+, CTest, Protobuf generic services, Muduo TCP server, Boost Serialization, POSIX file/process APIs, Python 3 standard library.

**Spec:** `docs/superpowers/specs/2026-08-31-raft-correctness-baseline-design.md`

## Global Constraints

- Preserve all pre-existing worktree changes. Task 1 deliberately adopts the current CTest and fiber-test changes; no task may reset or overwrite unrelated user work.
- Keep the existing `Clerk::Get`, `Put`, and `Append` source-compatible.
- Keep `raftCoreRun` as a convenience launcher while adding one-process-per-node execution.
- Use one explicit writable data directory per node. Construction must never truncate an existing WAL.
- Encode WAL header integers in big-endian order and use the exact version-1 record format in the spec.
- Use an in-tree CRC32 implementation; add no new C++ or Python package dependency.
- Call `fdatasync` before reporting a Raft state durable and fsync the parent directory after snapshot WAL rename.
- Treat complete-record checksum corruption as fatal; accept and truncate only an incomplete EOF tail.
- Run process-failure integration tests only on Linux/WSL and register them as skipped elsewhere.
- Give every process wait and client operation a finite deadline.
- Retain failed integration artifacts and remove successful artifacts.
- Do not add packet-loss simulation, ReadIndex, membership changes, sharding, or the full RPC framing redesign in this plan.

## File Structure

New production files:

- `src/raftCore/include/wal_format.h`: versioned WAL constants, record type, header encoding/decoding, and CRC32 declarations.
- `src/raftCore/wal_format.cpp`: binary record encoding, validation, and CRC32 implementation.
- `src/raftCore/include/snapshot_policy.h`: pure snapshot classification and retained-suffix planning types.
- `src/raftCore/snapshot_policy.cpp`: snapshot freshness and suffix-retention decisions.
- `src/common/include/cluster_config.h`: validated `NodeEndpoint` and cluster-config API.
- `src/common/cluster_config.cpp`: parser for contiguous `nodeNip/nodeNport` entries.
- `example/raftCoreExample/raftNode.cpp`: one-node process entry point.
- `example/raftCoreExample/kvctl.cpp`: bounded command-line KV and status client.

New test files:

- `test/persister_wal.cpp`: binary payload, reopen, compaction, torn-tail, and corruption tests.
- `test/skip_list_snapshot.cpp`: snapshot replacement and key/value restoration tests.
- `test/snapshot_policy.cpp`: stale, idempotent, conflicting, and retained-suffix tests.
- `test/cluster_config.cpp`: valid and malformed static configuration tests.
- `test/status_proto.cpp`: generated status service and field contract test.
- `test/integration/raft_cluster_test.py`: process controller and all black-box scenarios.

Existing files with responsibility changes:

- `src/raftCore/Persister.cpp` and `src/raftCore/include/Persister.h`: own and recover `<data-dir>/raft.wal`.
- `src/skipList/include/skipList.h`: make restore replace state without recursive locking and make reads thread-safe.
- `src/raftCore/raft.cpp` and `src/raftCore/include/raft.h`: expose status, commit a leader no-op, and order snapshot installation.
- `src/raftCore/kvServer.cpp` and `src/raftCore/include/kvServer.h`: explicit startup/data directory and status RPC implementation.
- `src/raftRpcPro/kvServerRPC.proto` plus generated `.pb.cc/.pb.h`: status messages and method.
- `src/rpc/rpcprovider.cpp` and `src/rpc/include/rpcprovider.h`: bind an explicit address without writing configuration.
- `src/rpc/mprpcchannel.cpp` and `src/rpc/include/mprpcchannel.h`: finite socket IO timeout and EOF handling needed by bounded clients.
- `src/raftClerk/clerk.cpp`, `src/raftClerk/include/clerk.h`, `src/raftClerk/raftServerRpcUtil.cpp`, and its header: deadline-aware operations and direct status RPC.
- `example/raftCoreExample/raftKvDB.cpp`: generate static configuration before forking compatibility children.
- `README.md`: manual node, client, and integration-test commands.
- `CMakeLists.txt`, component CMake files, and `test/CMakeLists.txt`: targets and CTest registration.

---

### Task 1: Adopt And Freeze The Existing Test Baseline

**Files:**
- Modify: `CMakeLists.txt`
- Modify: `example/fiberExample/test_scheduler.cpp`
- Modify: `example/fiberExample/test_thread.cc`
- Modify: `src/fiber/include/scheduler.hpp`
- Modify: `src/fiber/iomanager.cpp`
- Modify: `src/fiber/scheduler.cpp`
- Modify: `test/defer_run.cpp`
- Modify: `test/format.cpp`
- Modify: `test/run.cpp`
- Create: `test/CMakeLists.txt`
- Create: `test/iomanager_sleep.cpp`

**Interfaces:**
- Consumes: the current uncommitted baseline changes already present in the worktree.
- Produces: a six-test CTest baseline that later tasks must keep green.

- [ ] **Step 1: Inspect the exact baseline diff without changing it**

Run:

```bash
git status --short
git diff -- CMakeLists.txt example/fiberExample src/fiber test
```

Expected: only the previously established CTest/fiber changes appear; the committed design and plan documents are not mixed into this code diff.

- [ ] **Step 2: Configure and build the baseline**

Run:

```bash
cmake -S . -B cmake-build-debug -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON
cmake --build cmake-build-debug -j6
```

Expected: build succeeds and produces the existing example binaries plus the six finite test binaries.

- [ ] **Step 3: Run the baseline tests**

Run:

```bash
ctest --test-dir cmake-build-debug --output-on-failure
```

Expected: `6/6` tests pass, including `iomanager_cooperative_sleep` and `scheduler_plain_sleep`.

- [ ] **Step 4: Commit only the baseline files**

```bash
git add CMakeLists.txt example/fiberExample/test_scheduler.cpp example/fiberExample/test_thread.cc src/fiber/include/scheduler.hpp src/fiber/iomanager.cpp src/fiber/scheduler.cpp test
git commit -m "test: establish finite ctest baseline"
```

### Task 2: Replace Text Persistence With A Checksummed WAL

**Files:**
- Create: `src/raftCore/include/wal_format.h`
- Create: `src/raftCore/wal_format.cpp`
- Modify: `src/raftCore/include/Persister.h`
- Modify: `src/raftCore/Persister.cpp`
- Create: `test/persister_wal.cpp`
- Modify: `test/CMakeLists.txt`

**Interfaces:**
- Consumes: C++20 filesystem support and the existing `Persister::Save`, `SaveRaftState`, `ReadRaftState`, `ReadSnapshot`, and `RaftStateSize` call pattern.
- Produces: `Persister(std::filesystem::path dataDir)`, `kvraft::PersistenceError`, and durable version-1 WAL recovery while retaining `Persister(int me)` as a compatibility delegating constructor.

- [ ] **Step 1: Write the public WAL and persister contracts**

Add this shape to `wal_format.h`:

```cpp
#pragma once
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

namespace kvraft {
class PersistenceError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

namespace wal {
inline constexpr std::string_view kMagic = "KVRW";
inline constexpr std::uint16_t kVersion = 1;
inline constexpr std::size_t kHeaderSize = 28;
inline constexpr std::uint64_t kMaxPayloadBytes = 1ULL << 30;

enum class RecordType : std::uint16_t { State = 1, Snapshot = 2 };

struct Record {
  RecordType type;
  std::uint64_t sequence;
  std::string raftState;
  std::string snapshot;
};

std::uint32_t Crc32(std::string_view bytes);
std::string Encode(const Record& record);
Record Decode(std::string_view bytes);
}  // namespace wal
}  // namespace kvraft
```

Replace the stream-owning fields in `Persister.h` with this API:

```cpp
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
  void AppendRecord(kvraft::wal::RecordType type,
                    const std::string& raftState,
                    const std::string& snapshot);
  void CompactSnapshot(const std::string& raftState,
                       const std::string& snapshot);
  std::filesystem::path dataDir_;
  std::filesystem::path walPath_;
  int walFd_ = -1;
  std::uint64_t nextSequence_ = 1;
  std::string raftState_;
  std::string snapshot_;
  std::mutex mutex_;
};
```

- [ ] **Step 2: Write failing persistence tests**

Create `test/persister_wal.cpp` with a temporary-directory fixture and these assertions:

```cpp
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
```

Also add `TestPartialHeaderTail`, `TestPartialPayloadTail`, `TestCompleteChecksumCorruption`, `TestNonMonotonicSequence`, and `TestIndependentDirectories`. The two partial-tail cases append fewer than `kHeaderSize` bytes or a header with an incomplete payload and must reopen successfully at the preceding record. The checksum case flips one payload byte in a complete record and must catch `kvraft::PersistenceError`. The sequence case appends a valid record whose sequence is not greater than the prior record and must catch `kvraft::PersistenceError`. The directory case writes different payloads under `node-0` and `node-1`, reopens both, and verifies isolation.

- [ ] **Step 3: Register and run the failing test**

Add:

```cmake
add_executable(test_persister_wal persister_wal.cpp)
target_link_libraries(test_persister_wal PRIVATE skip_list_on_raft boost_serialization)
add_test(NAME persister_wal COMMAND $<TARGET_FILE:test_persister_wal>)
set_tests_properties(persister_wal PROPERTIES TIMEOUT 10)
```

Run:

```bash
cmake --build cmake-build-debug -j6 --target test_persister_wal
```

Expected: compilation fails because the path constructor, `WalPath`, and WAL types do not exist yet.

- [ ] **Step 4: Implement encoding and strict decoding**

In `wal_format.cpp`, use explicit `PutU16`, `PutU32`, `PutU64`, `ReadU16`, `ReadU32`, and `ReadU64` helpers. Encode the 28-byte header in big-endian order, set the checksum field to the CRC32 of bytes `[version..snapshot_len] + payload`, and reject:

```cpp
if (magic != kMagic) throw kvraft::PersistenceError("invalid WAL magic");
if (version != kVersion) throw kvraft::PersistenceError("unsupported WAL version");
if (type != State && type != Snapshot) throw kvraft::PersistenceError("invalid WAL record type");
if (raftLen + snapshotLen > kMaxPayloadBytes) throw kvraft::PersistenceError("WAL payload too large");
if (actualChecksum != expectedChecksum) throw kvraft::PersistenceError("WAL checksum mismatch");
```

Implement CRC32 with polynomial `0xEDB88320`, initial value `0xFFFFFFFF`, and final XOR `0xFFFFFFFF` so no external checksum library is needed.

- [ ] **Step 5: Implement append, sync, recovery, and snapshot compaction**

Use `open`, a complete-write loop, `fdatasync`, `ftruncate`, `rename`, and directory `fsync`. Fsync the directory after the first WAL creation. Recovery must track `lastValidOffset`, stop only for an incomplete EOF header/payload, truncate and sync that tail, and throw for any fully present invalid record. Remove a stale `raft.wal.tmp` only after the primary WAL has recovered successfully. `Save` writes that temporary WAL containing one snapshot record and atomically renames it; `SaveRaftState` appends a state record.

The compatibility constructor delegates exactly as follows:

```cpp
Persister::Persister(int me)
    : Persister(std::filesystem::path("data") /
                ("node-" + std::to_string(me))) {}
```

Do not read or migrate `raftstatePersistN.txt` or `snapshotPersistN.txt`. Those legacy files remain untouched and the new path-based launcher always points at the WAL data directory.

- [ ] **Step 6: Run focused and full tests**

Run:

```bash
cmake --build cmake-build-debug -j6
ctest --test-dir cmake-build-debug -R persister_wal --output-on-failure
ctest --test-dir cmake-build-debug --output-on-failure
```

Expected: WAL test and all baseline tests pass.

- [ ] **Step 7: Commit the WAL component**

```bash
git add src/raftCore/include/wal_format.h src/raftCore/wal_format.cpp src/raftCore/include/Persister.h src/raftCore/Persister.cpp test/persister_wal.cpp test/CMakeLists.txt
git commit -m "feat: add crash-safe raft wal"
```

### Task 3: Make KV Snapshot Restore Safe And Extract Snapshot Policy

**Files:**
- Modify: `src/skipList/include/skipList.h`
- Create: `src/raftCore/include/snapshot_policy.h`
- Create: `src/raftCore/snapshot_policy.cpp`
- Create: `test/skip_list_snapshot.cpp`
- Create: `test/snapshot_policy.cpp`
- Modify: `test/CMakeLists.txt`

**Interfaces:**
- Consumes: `SkipList::dump_file/load_file` and logical Raft log positions.
- Produces: thread-safe replace-on-restore behavior and `PlanSnapshotInstall(int, int, std::span<const LogPosition>, int, int) -> SnapshotInstallPlan` for Raft installation.

- [ ] **Step 1: Write the failing skip-list restore test**

Create `test/skip_list_snapshot.cpp`:

```cpp
int main() {
  SkipList<std::string, std::string> source(6);
  std::string alpha = "alpha";
  std::string value = "value-not-equal-to-key";
  source.insert_set_element(alpha, value);
  const std::string dump = source.dump_file();

  SkipList<std::string, std::string> restored(6);
  std::string stale = "stale";
  std::string staleValue = "remove-me";
  restored.insert_set_element(stale, staleValue);
  restored.load_file(dump);

  std::string actual;
  if (!restored.search_element(alpha, actual) || actual != value) return 1;
  if (restored.search_element(stale, actual)) return 2;
  return 0;
}
```

Register it with a five-second CTest timeout. Run:

```bash
cmake --build cmake-build-debug -j6 --target test_skip_list_snapshot
ctest --test-dir cmake-build-debug -R skip_list_snapshot --output-on-failure
```

Expected: timeout from recursive locking or failure because the restored value equals the key and stale state remains.

- [ ] **Step 2: Write the failing pure snapshot-policy test**

Define the expected interface in `test/snapshot_policy.cpp`:

```cpp
using kvraft::SnapshotDecision;
using kvraft::SnapshotInstallPlan;
using kvraft::LogPosition;
using kvraft::PlanSnapshotInstall;

const std::vector<LogPosition> logs{{11, 4}, {12, 4}, {13, 5}};
Expect(PlanSnapshotInstall(10, 3, logs, 9, 2).decision == SnapshotDecision::Stale);
Expect(PlanSnapshotInstall(10, 3, logs, 10, 3).decision == SnapshotDecision::Idempotent);
Expect(PlanSnapshotInstall(10, 3, logs, 10, 4).decision == SnapshotDecision::Conflict);

auto retain = PlanSnapshotInstall(10, 3, logs, 12, 4);
Expect(retain.decision == SnapshotDecision::Install);
Expect(retain.firstRetainedLog == 2);

auto discard = PlanSnapshotInstall(10, 3, logs, 12, 9);
Expect(discard.decision == SnapshotDecision::Install);
Expect(discard.firstRetainedLog == logs.size());
```

Register both targets:

```cmake
add_executable(test_skip_list_snapshot skip_list_snapshot.cpp)
target_link_libraries(test_skip_list_snapshot PRIVATE boost_serialization)
add_test(NAME skip_list_snapshot COMMAND $<TARGET_FILE:test_skip_list_snapshot>)

add_executable(test_snapshot_policy snapshot_policy.cpp ../src/raftCore/snapshot_policy.cpp)
add_test(NAME snapshot_policy COMMAND $<TARGET_FILE:test_snapshot_policy>)
set_tests_properties(skip_list_snapshot snapshot_policy PROPERTIES TIMEOUT 5)
```

Expected before implementation: skip-list restore times out or returns non-zero, and snapshot policy compilation fails because `snapshot_policy.h` is absent.

- [ ] **Step 3: Implement single-lock skip-list operations**

Add private `insertElementUnlocked`, `clearUnlocked`, and `findNodeUnlocked` helpers. Public methods acquire the mutex exactly once. `insert_set_element` updates an existing node in place instead of composing `search_element`, `delete_element`, and `insert_element`. `load_file` deserializes before locking, clears the existing list under the lock, and inserts each `keyDumpVt_[i]` with `valDumpVt_[i]` through the unlocked helper.

Protect `search_element`, `size`, `display_list`, and destruction consistently. Use RAII locks so exceptions cannot leave the list locked.

- [ ] **Step 4: Implement the snapshot policy**

Create:

```cpp
namespace kvraft {
enum class SnapshotDecision { Stale, Idempotent, Conflict, Install };
struct LogPosition { int index; int term; };
struct SnapshotInstallPlan {
  SnapshotDecision decision;
  std::size_t firstRetainedLog;
};

SnapshotInstallPlan PlanSnapshotInstall(
    int localSnapshotIndex,
    int localSnapshotTerm,
    std::span<const LogPosition> logs,
    int incomingIndex,
    int incomingTerm);
}  // namespace kvraft
```

For a newer snapshot, retain the suffix only when the local log contains the incoming index with the incoming term; `firstRetainedLog` is one past that matching entry. Otherwise return `logs.size()` to discard all current log entries.

- [ ] **Step 5: Run focused and full tests**

```bash
cmake --build cmake-build-debug -j6
ctest --test-dir cmake-build-debug -R "skip_list_snapshot|snapshot_policy" --output-on-failure
ctest --test-dir cmake-build-debug --output-on-failure
```

Expected: both new tests and the baseline pass without timeout.

- [ ] **Step 6: Commit snapshot foundations**

```bash
git add src/skipList/include/skipList.h src/raftCore/include/snapshot_policy.h src/raftCore/snapshot_policy.cpp test/skip_list_snapshot.cpp test/snapshot_policy.cpp test/CMakeLists.txt
git commit -m "fix: make snapshot restoration deterministic"
```

### Task 4: Add Raft Status And Correct Recovery Semantics

**Files:**
- Modify: `src/raftCore/include/raft.h`
- Modify: `src/raftCore/raft.cpp`
- Modify: `src/raftCore/include/kvServer.h`
- Modify: `src/raftCore/kvServer.cpp`
- Modify: `src/raftRpcPro/kvServerRPC.proto`
- Regenerate: `src/raftRpcPro/kvServerRPC.pb.h`
- Regenerate: `src/raftRpcPro/kvServerRPC.pb.cc`
- Modify: `src/raftClerk/include/raftServerRpcUtil.h`
- Modify: `src/raftClerk/raftServerRpcUtil.cpp`
- Create: `test/status_proto.cpp`
- Modify: `test/CMakeLists.txt`

**Interfaces:**
- Consumes: `PlanSnapshotInstall`, durable `Persister`, and existing protobuf generic services.
- Produces: `RaftStatus Raft::GetStatusSnapshot()`, `GetStatus` RPC, ordered snapshot application, and current-term no-op commitment.

- [ ] **Step 1: Add a failing protobuf contract test**

Create `test/status_proto.cpp`:

```cpp
int main() {
  const auto* service = raftKVRpcProctoc::kvServerRpc::descriptor();
  if (service->FindMethodByName("GetStatus") == nullptr) return 1;
  const auto* reply = raftKVRpcProctoc::StatusReply::descriptor();
  for (const char* name : {"node_id", "term", "role", "commit_index",
                           "last_applied", "snapshot_index", "snapshot_term"}) {
    if (reply->FindFieldByName(name) == nullptr) return 2;
  }
  return 0;
}
```

Register it:

```cmake
add_executable(test_status_proto status_proto.cpp ${src_raftRpcPro})
target_link_libraries(test_status_proto PRIVATE protobuf)
add_test(NAME status_proto COMMAND $<TARGET_FILE:test_status_proto>)
set_tests_properties(status_proto PROPERTIES TIMEOUT 5)
```

Expected: compilation fails because `StatusReply` and `GetStatus` are absent.

- [ ] **Step 2: Extend and regenerate the service protobuf**

Append this contract to `kvServerRPC.proto` and add the method to `kvServerRpc`:

```proto
enum NodeRole {
  FOLLOWER = 0;
  CANDIDATE = 1;
  LEADER = 2;
}

message StatusArgs {}
message StatusReply {
  int32 node_id = 1;
  int32 term = 2;
  NodeRole role = 3;
  int32 commit_index = 4;
  int32 last_applied = 5;
  int32 snapshot_index = 6;
  int32 snapshot_term = 7;
}
```

Run:

```bash
protoc -I src/raftRpcPro --cpp_out=src/raftRpcPro src/raftRpcPro/kvServerRPC.proto
```

- [ ] **Step 3: Add a mutex-protected Raft status value**

Add public types and method:

```cpp
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

RaftStatus GetStatusSnapshot();
```

Map the internal status enum under `m_mtx` and return a by-value snapshot. Keep `GetState` for existing callers.

- [ ] **Step 4: Append and apply a current-term no-op**

When `sendRequestVote` first reaches a majority, initialize leader replication indices from the pre-no-op last index, append an `Op` with these exact fields, persist it, and then trigger the immediate heartbeat:

```cpp
Op noOp;
noOp.Operation = "NoOp";
noOp.Key.clear();
noOp.Value.clear();
noOp.ClientId = "raft-internal";
noOp.RequestId = m_currentTerm;
```

`KvServer::GetCommandFromRaft` must recognize `NoOp`, make no KV mutation, still advance snapshot checks, and not create a client wait channel.

During `Raft::init`, set both `m_commitIndex` and `m_lastApplied` to `m_lastSnapshotIncludeIndex` after persistent state recovery.

- [ ] **Step 5: Integrate snapshot planning and queue ordering**

In `InstallSnapshot`, build `LogPosition` values from `m_logs`, call `PlanSnapshotInstall`, and handle decisions as follows:

```cpp
Stale       -> set reply term and return without mutation
Idempotent  -> set reply term and return without a second apply message
Conflict    -> throw kvraft::PersistenceError("snapshot term conflicts at installed index")
Install     -> retain logs[firstRetainedLog..end], update boundary,
               persist state+snapshot, update volatile indices, enqueue ApplyMsg
```

Enqueue the snapshot directly while Raft still excludes concurrent log extraction; remove the detached `pushMsgToKvServer` thread so a later command cannot enter `applyChan` first. Implement `CondInstallSnapshot` as an equality/freshness guard: accept only the currently installed index and term, reject an older message, and reject a conflicting term.

- [ ] **Step 6: Implement and expose `GetStatus`**

Add local and protobuf override overloads to `KvServer`, fill every response field from `GetStatusSnapshot`, and add `raftServerRpcUtil::GetStatus(StatusReply*)` for a direct-node call.

- [ ] **Step 7: Build and run status/snapshot tests**

```bash
cmake --build cmake-build-debug -j6
ctest --test-dir cmake-build-debug -R "status_proto|snapshot_policy|persister_wal" --output-on-failure
ctest --test-dir cmake-build-debug --output-on-failure
```

Expected: generated protobuf contract and all existing tests pass.

- [ ] **Step 8: Commit Raft recovery behavior**

```bash
git add src/raftCore/include/raft.h src/raftCore/raft.cpp src/raftCore/include/kvServer.h src/raftCore/kvServer.cpp src/raftRpcPro/kvServerRPC.proto src/raftRpcPro/kvServerRPC.pb.h src/raftRpcPro/kvServerRPC.pb.cc src/raftClerk/include/raftServerRpcUtil.h src/raftClerk/raftServerRpcUtil.cpp test/status_proto.cpp test/CMakeLists.txt
git commit -m "feat: expose raft status and restore snapshot ordering"
```

### Task 5: Add Static Cluster Configuration And One-Node Runtime

**Files:**
- Create: `src/common/include/cluster_config.h`
- Create: `src/common/cluster_config.cpp`
- Modify: `src/raftCore/include/raft.h`
- Modify: `src/raftCore/raft.cpp`
- Modify: `src/raftCore/include/kvServer.h`
- Modify: `src/raftCore/kvServer.cpp`
- Modify: `src/rpc/include/rpcprovider.h`
- Modify: `src/rpc/rpcprovider.cpp`
- Modify: `src/raftCore/raftRpcUtil.cpp`
- Create: `example/raftCoreExample/raftNode.cpp`
- Modify: `example/raftCoreExample/raftKvDB.cpp`
- Modify: `example/raftCoreExample/CMakeLists.txt`
- Modify: `example/rpcExample/callee/friendService.cpp`
- Create: `test/cluster_config.cpp`
- Modify: `test/CMakeLists.txt`

**Interfaces:**
- Consumes: explicit per-node `Persister` paths and current `nodeNip/nodeNport` config format.
- Produces: `LoadClusterConfig(path)`, `Raft::StartBackgroundTasks()`, blocking `KvServer::StartKVServer()`, and `raftNode` CLI.

- [ ] **Step 1: Write failing configuration tests**

Define:

```cpp
struct NodeEndpoint { std::string ip; std::uint16_t port; };
std::vector<NodeEndpoint> LoadClusterConfig(const std::filesystem::path& path);
```

Test a valid three-node file, a missing `node1port`, a non-numeric port, port zero, port above 65535, and a gap between `node0` and `node2`. Valid input returns endpoints in node-id order; every malformed case throws `std::runtime_error` with the key name in the message.

Run the new target and expect compilation failure before `cluster_config.h` exists.

Register it against the common sources:

```cmake
add_executable(test_cluster_config cluster_config.cpp ${src_common})
add_test(NAME cluster_config COMMAND $<TARGET_FILE:test_cluster_config>)
set_tests_properties(cluster_config PROPERTIES TIMEOUT 5)
```

- [ ] **Step 2: Implement strict static configuration**

Parse trimmed non-comment `key=value` lines, reject duplicate keys, require at least one node, require contiguous ids starting at zero, and use `std::from_chars` for ports. Do not call `exit` from the parser.

- [ ] **Step 3: Stop the RPC provider from editing configuration**

Change:

```cpp
void RpcProvider::Run(const std::string& bindIp, std::uint16_t port);
```

Remove hostname lookup and all writes to `test.conf`. Bind exactly `bindIp:port`. Update the standalone RPC example to `provider.Run("127.0.0.1", 7788)`.

- [ ] **Step 4: Separate `KvServer` construction from blocking startup**

Use this constructor:

```cpp
KvServer(int me,
         int maxRaftState,
         std::filesystem::path configPath,
         std::filesystem::path dataDir);
void StartKVServer();  // blocks in RpcProvider::Run
```

Split `Raft::init` so it initializes dependencies and recovers persistent Raft state without starting timers. Add `Raft::StartBackgroundTasks()` containing the IO manager schedulers and applier thread currently at the end of `init`.

Construction validates configuration, creates `Persister(dataDir)`, calls the recovery-only `Raft::init`, restores the KV snapshot, builds lazy peer RPC clients, and starts the apply consumer. `StartKVServer` calls `Raft::StartBackgroundTasks()` only after all recovery steps, then publishes the KV and Raft services on the endpoint selected by `me`. Remove fixed startup sleeps and change `RaftRpcUtil` to construct the channel lazily with `MprpcChannel(peerIp, peerPort, false)`.

- [ ] **Step 5: Add `raftNode` argument parsing**

Require all four options, support `--help`, reject negative ids and non-positive snapshot thresholds, and return non-zero on configuration/storage exceptions. Main must be equivalent to:

```cpp
KvServer server(args.id, args.maxRaftState, args.config, args.dataDir);
server.StartKVServer();
```

Add a CTest smoke test:

```cmake
add_executable(raftNode raftNode.cpp)
target_link_libraries(raftNode PRIVATE skip_list_on_raft rpc_lib protobuf
                                       muduo_net muduo_base pthread)
add_test(NAME raft_node_help COMMAND $<TARGET_FILE:raftNode> --help)
set_tests_properties(raft_node_help PROPERTIES TIMEOUT 5)
```

- [ ] **Step 6: Make the compatibility launcher static-first**

`raftCoreRun` chooses all ports, writes the complete configuration once, creates one data subdirectory per node, and only then forks children. Each child constructs `KvServer(i, maxRaftState, configPath, dataRoot/node-i)` and calls `StartKVServer`. The provider never appends configuration.

- [ ] **Step 7: Build and run component tests**

```bash
cmake --build cmake-build-debug -j6
ctest --test-dir cmake-build-debug -R "cluster_config|raft_node_help|persister_wal" --output-on-failure
ctest --test-dir cmake-build-debug --output-on-failure
```

Expected: valid config and CLI help pass; malformed config cases return controlled failures; all baseline tests remain green.

- [ ] **Step 8: Commit the node runtime**

```bash
git add src/common/include/cluster_config.h src/common/cluster_config.cpp src/raftCore/include/raft.h src/raftCore/raft.cpp src/raftCore/include/kvServer.h src/raftCore/kvServer.cpp src/raftCore/raftRpcUtil.cpp src/rpc/include/rpcprovider.h src/rpc/rpcprovider.cpp example/raftCoreExample/raftNode.cpp example/raftCoreExample/raftKvDB.cpp example/raftCoreExample/CMakeLists.txt example/rpcExample/callee/friendService.cpp test/cluster_config.cpp test/CMakeLists.txt
git commit -m "feat: run raft nodes as independent processes"
```

### Task 6: Add Deadline-Aware Clerk Operations And `kvctl`

**Files:**
- Modify: `src/rpc/include/mprpcchannel.h`
- Modify: `src/rpc/mprpcchannel.cpp`
- Modify: `src/raftClerk/include/clerk.h`
- Modify: `src/raftClerk/clerk.cpp`
- Modify: `src/raftClerk/include/raftServerRpcUtil.h`
- Modify: `src/raftClerk/raftServerRpcUtil.cpp`
- Create: `example/raftCoreExample/kvctl.cpp`
- Modify: `example/raftCoreExample/CMakeLists.txt`
- Modify: `test/CMakeLists.txt`

**Interfaces:**
- Consumes: static cluster config and `GetStatus` RPC.
- Produces: bounded `ClerkStatus` methods and stable `kvctl` exit/output behavior.

- [ ] **Step 1: Define bounded Clerk results**

Add:

```cpp
enum class ClerkStatus { Ok, NotFound, TimedOut, Unavailable };

ClerkStatus GetUntil(const std::string& key,
                     std::string* value,
                     std::chrono::steady_clock::time_point deadline);
ClerkStatus PutUntil(const std::string& key,
                     const std::string& value,
                     std::chrono::steady_clock::time_point deadline);
ClerkStatus AppendUntil(const std::string& key,
                        const std::string& value,
                        std::chrono::steady_clock::time_point deadline);
```

Existing methods call the same retry loop with `time_point::max()`. Bounded loops check the deadline before every attempt, sleep 20 ms after a complete unsuccessful server cycle, preserve one request id across retries, and return `Unavailable` immediately when the configuration has no servers.

- [ ] **Step 2: Bound individual socket operations**

Extend `MprpcChannel` with an IO timeout argument defaulting to 300 ms. Implement connect with `O_NONBLOCK`, `poll(POLLOUT)` using that timeout, and `getsockopt(SO_ERROR)` before restoring the socket flags. Apply `SO_SNDTIMEO` and `SO_RCVTIMEO`, loop until the complete request buffer is sent, retry `send/recv` only on `EINTR`, treat `recv == 0` as connection closure, close the fd after timeout/error, and set the controller failure. This task does not redesign response framing.

- [ ] **Step 3: Add `kvctl` and smoke tests**

Implement exact commands:

```text
kvctl --config PATH --timeout-ms N put KEY VALUE
kvctl --config PATH --timeout-ms N get KEY
kvctl --config PATH --timeout-ms N status --node N
```

Exit codes are `0` success, `2` missing key, `3` timeout/unavailable, and `64` usage/config error. `get` writes only the raw value plus newline. `status` writes:

```text
node_id=N term=N role=FOLLOWER|CANDIDATE|LEADER commit_index=N last_applied=N snapshot_index=N snapshot_term=N
```

Register `kvctl --help` and an invalid-command CTest whose `WILL_FAIL` property is true.

Add the executable with the Clerk sources explicitly, matching `callerMain`:

```cmake
add_executable(kvctl kvctl.cpp ${src_raftClerk} ${src_common})
target_link_libraries(kvctl PRIVATE skip_list_on_raft protobuf boost_serialization)
add_test(NAME kvctl_help COMMAND $<TARGET_FILE:kvctl> --help)
add_test(NAME kvctl_invalid COMMAND $<TARGET_FILE:kvctl> invalid)
set_tests_properties(kvctl_invalid PROPERTIES WILL_FAIL TRUE)
set_tests_properties(kvctl_help kvctl_invalid PROPERTIES TIMEOUT 5)
```

- [ ] **Step 4: Build and run CLI tests**

```bash
cmake --build cmake-build-debug -j6
ctest --test-dir cmake-build-debug -R "kvctl|raft_node_help|status_proto" --output-on-failure
ctest --test-dir cmake-build-debug --output-on-failure
```

Expected: CLI help succeeds, malformed usage fails promptly, and all component tests pass.

- [ ] **Step 5: Commit bounded client tooling**

```bash
git add src/rpc/include/mprpcchannel.h src/rpc/mprpcchannel.cpp src/raftClerk/include/clerk.h src/raftClerk/clerk.cpp src/raftClerk/include/raftServerRpcUtil.h src/raftClerk/raftServerRpcUtil.cpp example/raftCoreExample/kvctl.cpp example/raftCoreExample/CMakeLists.txt test/CMakeLists.txt
git commit -m "feat: add bounded raft client tooling"
```

### Task 7: Build The Multi-Process Election And Failover Harness

**Files:**
- Create: `test/integration/raft_cluster_test.py`
- Modify: `test/CMakeLists.txt`

**Interfaces:**
- Consumes: `raftNode`, `kvctl`, static configuration, stable status output, and per-node data directories.
- Produces: reusable `RaftCluster` process controller and election, replication, leader-failure, and follower-catchup scenarios.

- [ ] **Step 1: Implement the process controller**

Create these concrete process operations:

```python
@dataclasses.dataclass
class NodeProcess:
    node_id: int
    data_dir: pathlib.Path
    log_path: pathlib.Path
    process: subprocess.Popen[str] | None = None

class RaftCluster:
    def start_node(self, node_id: int) -> None:
        node = self.nodes[node_id]
        command = [self.raft_node, "--id", str(node_id),
                   "--config", str(self.config_path),
                   "--data-dir", str(node.data_dir),
                   "--max-raft-state", str(self.max_raft_state)]
        log = node.log_path.open("a", encoding="utf-8")
        node.process = subprocess.Popen(
            command, stdout=log, stderr=subprocess.STDOUT,
            text=True, start_new_session=True)
        log.close()

    def stop_node(self, node_id: int,
                  sig: signal.Signals = signal.SIGTERM) -> None:
        process = self.nodes[node_id].process
        if process is None or process.poll() is not None:
            return
        os.killpg(process.pid, sig)
        try:
            process.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=3.0)

    def restart_node(self, node_id: int) -> None:
        self.stop_node(node_id)
        self.start_node(node_id)

    def status(self, node_id: int, timeout: float = 1.0) -> dict[str, str]:
        completed = subprocess.run(
            [self.kvctl, "--config", str(self.config_path),
             "--timeout-ms", str(int(timeout * 1000)),
             "status", "--node", str(node_id)],
            text=True, capture_output=True,
            timeout=timeout + 1.0, check=True)
        return dict(field.split("=", 1)
                    for field in completed.stdout.strip().split())

    def wait_for_leader(self, timeout: float = 10.0) -> tuple[int, int]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            leaders = []
            for node_id in self.live_node_ids():
                try:
                    current = self.status(node_id)
                except (subprocess.SubprocessError, OSError, ValueError):
                    continue
                if current["role"] == "LEADER":
                    leaders.append((node_id, int(current["term"])))
            if len(leaders) == 1:
                node_id, term = leaders[0]
                self.record_leader(term, node_id)
                return node_id, term
            time.sleep(0.05)
        raise AssertionError("leader election exceeded 10 seconds")

    def put(self, key: str, value: str, timeout: float = 5.0) -> None:
        self.run_kvctl(["put", key, value], timeout)

    def get(self, key: str, timeout: float = 5.0) -> str:
        return self.run_kvctl(["get", key], timeout).stdout.rstrip("\n")

    def cleanup(self) -> None:
        for node_id in self.live_node_ids():
            self.stop_node(node_id)
```

`live_node_ids` returns ids whose `Popen` has not exited. `run_kvctl` adds the common config and timeout arguments and calls `subprocess.run` with captured output and `check=True`. `record_leader` stores `term -> node_id` and raises when the same term was already observed with a different id. Reserve three loopback ports with bound sockets, write all endpoints, close the sockets, and immediately start the nodes. Retry the whole allocation once if a node log reports `Address already in use`.

The script entry point creates `<artifact-root>/<scenario>-<timestamp>-<pid>`, sets `success = False`, and wraps scenario execution in `try/finally`. The `finally` block always calls `cluster.cleanup()`. Set `success = True` only after all assertions; remove the artifact directory only when `success` is true, otherwise print its absolute path to stderr before re-raising the failure.

- [ ] **Step 2: Add election and replication scenarios**

`scenario_election` starts three nodes and requires one leader. `scenario_replication` writes keys `key-000` through `key-099`, verifies corresponding values `value-000` through `value-099`, and waits until every live node has `last_applied >=` the leader's committed position.

- [ ] **Step 3: Add leader failure and follower catchup**

Kill the current leader with `SIGKILL`, require a higher-term leader among the two survivors, verify the first 100 keys, write `after-failover-000` through `after-failover-049`, restart the killed node with the same data directory, and wait until its `commit_index` and `last_applied` reach the live leader's values.

- [ ] **Step 4: Register the first integration test**

```cmake
find_package(Python3 COMPONENTS Interpreter)
if(Python3_Interpreter_FOUND)
  function(add_raft_integration_test test_name scenario_name)
    add_test(
      NAME ${test_name}
      COMMAND ${Python3_EXECUTABLE}
              ${CMAKE_CURRENT_SOURCE_DIR}/integration/raft_cluster_test.py
              --scenario ${scenario_name}
              --raft-node $<TARGET_FILE:raftNode>
              --kvctl $<TARGET_FILE:kvctl>
              --artifact-root ${CMAKE_CURRENT_BINARY_DIR}/artifacts)
    set_tests_properties(${test_name} PROPERTIES
      LABELS raft_integration
      TIMEOUT 120
      SKIP_RETURN_CODE 77)
  endfunction()
  add_raft_integration_test(raft_integration_failover failover)
else()
  message(STATUS "Python 3 not found: Raft integration tests unavailable")
endif()
```

Before argument parsing, the Python entry point returns `77` when `sys.platform` does not start with `"linux"`; CTest therefore reports the registered scenario as skipped on unsupported systems.

- [ ] **Step 5: Run the integration test with its fixed deadline**

```bash
cmake --build cmake-build-debug -j6
ctest --test-dir cmake-build-debug -R raft_integration_failover --output-on-failure
```

Expected: PASS within the 120-second CTest timeout, with one higher-term leader after `SIGKILL`, all 150 values readable, the restarted node caught up, and no child process left running. A failing run must still exit within the timeout and print its retained artifact path; the 10-second election deadline is an assertion and must not be increased.

- [ ] **Step 6: Run all tests and commit the harness**

```bash
ctest --test-dir cmake-build-debug --output-on-failure
git add test/integration/raft_cluster_test.py test/CMakeLists.txt
git commit -m "test: cover raft election and leader failover"
```

Expected: the failover scenario and all component tests pass with no controlled process left alive.

### Task 8: Add Full Restart, Snapshot Recovery, And Torn-Tail Scenarios

**Files:**
- Modify: `test/integration/raft_cluster_test.py`
- Modify: `test/CMakeLists.txt`

**Interfaces:**
- Consumes: `RaftCluster`, leader no-op behavior, WAL recovery, and InstallSnapshot.
- Produces: the remaining five black-box scenarios from the approved design.

- [ ] **Step 1: Add a full-cluster restart test**

Write 100 keys, save the highest observed committed index, `SIGKILL` all three nodes, restart all three with unchanged data directories, and wait for `last_applied` to reach the saved index before issuing any user write. Then read all 100 values.

Run:

```bash
ctest --test-dir cmake-build-debug -R raft_integration_restart --output-on-failure
```

Expected: PASS without a user write advancing commitment; the current-term no-op from Task 4 must move `last_applied` to the saved committed index.

- [ ] **Step 2: Add snapshot restart**

Start nodes with `--max-raft-state 4096`, write distinct values until every live node reports `snapshot_index > 0`, kill all nodes, restart them, wait for leadership, and read a sample covering the first, middle, and final written keys.

- [ ] **Step 3: Add InstallSnapshot follower catchup**

Stop one follower, record its applied index, continue unique writes until the leader's `snapshot_index` is greater than that recorded index, restart the follower, and require both its `snapshot_index` and `last_applied` to catch up. Kill the current leader, wait for quorum leadership, and verify the complete data set through normal reads.

- [ ] **Step 4: Add torn WAL tail recovery**

Stop one caught-up follower, append exactly `b"KVRW\x00\x01"` to its `raft.wal`, restart it, and require it to rejoin and catch up. This is an incomplete header, not a checksum-corrupt complete record, so recovery must truncate it rather than fail startup.

- [ ] **Step 5: Add fatal complete-corruption process behavior**

Copy a stopped node's WAL to an isolated data directory, flip one byte in the final complete record payload, and start `raftNode` against that directory and a valid config. Require a non-zero exit within five seconds and stderr containing `checksum`.

- [ ] **Step 6: Register each scenario separately**

Reuse the CMake helper from Task 7:

```cmake
if(Python3_Interpreter_FOUND)
  add_raft_integration_test(raft_integration_restart restart)
  add_raft_integration_test(raft_integration_snapshot_restart snapshot_restart)
  add_raft_integration_test(raft_integration_snapshot_catchup snapshot_catchup)
  add_raft_integration_test(raft_integration_wal_tail wal_tail)
endif()
```

The `wal_tail` scenario performs both the recoverable incomplete-tail check and the fatal complete-checksum-corruption check so the four test names cover all five Task 8 assertions.

- [ ] **Step 7: Run the complete integration label**

```bash
cmake --build cmake-build-debug -j6
ctest --test-dir cmake-build-debug -L raft_integration --output-on-failure
ctest --test-dir cmake-build-debug --output-on-failure
```

Expected: all integration scenarios and component tests pass; failure artifacts are absent after a successful run.

- [ ] **Step 8: Commit recovery scenarios and required fixes**

```bash
git add test/integration/raft_cluster_test.py test/CMakeLists.txt
git commit -m "test: verify raft restart and snapshot recovery"
```

### Task 9: Document Operations And Run The Acceptance Loop

**Files:**
- Modify: `README.md`
- Modify: `test/测试文件运行说明.md`
- Modify: `docs/项目阅读指南.md`

**Interfaces:**
- Consumes: final `raftNode`, `kvctl`, CTest names, and artifact behavior.
- Produces: reproducible manual instructions and verified acceptance evidence.

- [ ] **Step 1: Add exact manual cluster commands**

Document a three-terminal example using a complete configuration:

```text
node0ip=127.0.0.1
node0port=19000
node1ip=127.0.0.1
node1port=19001
node2ip=127.0.0.1
node2port=19002
```

Document one command per node with separate `data/node-N` directories, followed by `kvctl put`, `kvctl get`, and `kvctl status --node N`. State explicitly that reusing the same data directory performs recovery and deleting it creates a fresh node.

- [ ] **Step 2: Document test selection and failure artifacts**

Include:

```bash
ctest --test-dir cmake-build-debug --output-on-failure
ctest --test-dir cmake-build-debug -L raft_integration --output-on-failure
ctest --test-dir cmake-build-debug -L raft_integration --repeat until-fail:10 --output-on-failure
```

Explain the artifact directory printed on failure and the per-node log/status history it contains.

- [ ] **Step 3: Run format and diff checks**

```bash
cmake --build cmake-build-debug --target format
git diff --check
```

Expected: formatting completes and no whitespace errors are reported. Inspect formatter output before staging so unrelated files are not included.

- [ ] **Step 4: Run the complete acceptance suite**

```bash
cmake --build cmake-build-debug -j6
ctest --test-dir cmake-build-debug --output-on-failure
ctest --test-dir cmake-build-debug -L raft_integration --repeat until-fail:10 --output-on-failure
```

Expected: every component test passes, then the entire integration label passes ten consecutive iterations without timeout or residual node process.

- [ ] **Step 5: Verify no controlled process remains**

Run:

```bash
pgrep -af '/bin/raftNode' || true
```

Expected: no process from the integration artifact directories is listed. Do not terminate unrelated user processes; inspect any result by command line and PID first.

- [ ] **Step 6: Commit documentation and final verification state**

```bash
git add README.md test/测试文件运行说明.md docs/项目阅读指南.md
git commit -m "docs: explain raft recovery testing"
```

Record the final test counts and ten-run integration result in the completion report; do not add generated logs or test artifacts to git.
