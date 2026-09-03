# Raft Transport and Correctness Hardening - Implementation Plan

> **For implementation:** Execute this plan task by task using `superpowers:test-driven-development`.

**Goal:** Make RPC transport stream-safe and serialized, correct Raft snapshot/log conflict behavior, and add deterministic fault and concurrency coverage.

**Architecture:** Wrap every existing protobuf RPC payload in a 4-byte big-endian length frame. The server consumes complete frames incrementally from Muduo's buffer; the client serializes each channel transaction with one mutex and performs exact-length reads. Raft keeps valid matching suffixes, truncating only at the first term conflict. A test-only outbound Raft RPC fault policy makes partition, latency, drop, and duplicate paths reproducible.

**Tech Stack:** C++17, Muduo, Protocol Buffers, CMake/CTest, Python 3 integration tests.

**Scope constraints:** All cluster processes upgrade together; legacy unframed wire compatibility is intentionally not retained. `RaftRpcUtil`'s existing per-peer try-lock remains as a scheduling guard; `MprpcChannel` becomes safe for all callers itself. Fault injection is enabled only by an explicitly supplied policy file.

---

## Task 1: Add framed RPC transport primitives and server stream parsing

**Files:**
- Create: `src/rpc/include/rpc_frame.h`
- Create: `src/rpc/rpc_frame.cpp`
- Modify: `src/rpc/rpcprovider.cpp`
- Modify: `src/rpc/CMakeLists.txt` only if automatic source discovery does not include the new source
- Create: `test/rpc_frame_test.cpp`
- Modify: `test/CMakeLists.txt`

**Interfaces:**
```cpp
namespace mprpc {
constexpr std::uint32_t kMaxRpcFrameSize = 16 * 1024 * 1024;
std::string EncodeRpcFrame(const std::string& payload);
bool TryConsumeRpcFrame(muduo::net::Buffer* buffer, std::string* payload);
}
```

- [ ] **Step 1: Write the failing unit tests.** Cover a split frame (incomplete header and incomplete body leave the buffer untouched), two coalesced frames (consume exactly one at a time), a zero-length payload, and an oversized declared length rejection.
- [ ] **Step 2: Run the focused test and confirm RED.** Configure the existing build if required, then run `ctest --test-dir cmake-build-debug -R rpc_frame_test --output-on-failure`; it must fail because the framing API is absent.
- [ ] **Step 3: Implement the framing API.** Encode a network-byte-order `uint32_t` followed by payload. Inspect buffer readable bytes without retrieval until a complete valid frame exists; reject lengths above the limit deterministically.
- [ ] **Step 4: Convert `RpcProvider::OnMessage`.** Loop while `TryConsumeRpcFrame` yields a payload. Parse the existing varint-header/request payload from that isolated frame and send one framed response for each request. On malformed content, log and close the connection instead of desynchronizing the stream.
- [ ] **Step 5: Run focused tests GREEN.** Run `ctest --test-dir cmake-build-debug -R rpc_frame_test --output-on-failure`.
- [ ] **Step 6: Commit.** Stage the framing sources, provider change, and unit test; commit with `feat: frame RPC requests and responses`.

## Task 2: Serialize `MprpcChannel` and support complete framed responses

**Files:**
- Modify: `src/rpc/include/mprpcchannel.h`
- Modify: `src/rpc/mprpcchannel.cpp`
- Modify: `test/rpc_frame_test.cpp`

**Interfaces:**
```cpp
std::mutex m_callMutex;
bool SendAll(const char* data, std::size_t size);
bool RecvExact(char* data, std::size_t size);
```

- [ ] **Step 1: Extend the failing tests.** Use a loopback test server that fragments a response header/body and sends a payload larger than 1024 bytes. Add two simultaneous calls through one `MprpcChannel` and assert each receives its own expected protobuf response.
- [ ] **Step 2: Run the focused test and confirm RED.** Run `ctest --test-dir cmake-build-debug -R rpc_frame_test --output-on-failure`; legacy single-recv behavior must fail the large/fragmented response case.
- [ ] **Step 3: Make an RPC transaction atomic.** Lock `m_callMutex` at the start of `CallMethod` through reconnect, full framed send, full framed receive, and response parsing. Replace the fixed receive buffer with exact reads of the 4-byte prefix then declared payload; reject invalid or oversized response lengths and invalidate the connection on transport failure.
- [ ] **Step 4: Preserve controller semantics.** All errors call `controller->SetFailed`, leave `done` behavior unchanged, and allow the next call to reconnect cleanly.
- [ ] **Step 5: Run focused tests GREEN.** Run `ctest --test-dir cmake-build-debug -R rpc_frame_test --output-on-failure`.
- [ ] **Step 6: Add a real large-value integration assertion.** In `test/integration/raft_cluster_test.py`, write and read a value larger than 1024 bytes, then run `ctest --test-dir cmake-build-debug -R raft_cluster_failover --output-on-failure`.
- [ ] **Step 7: Commit.** Stage channel, transport tests, and integration test; commit with `fix: serialize framed RPC channel calls`.

## Task 3: Correct AppendEntries snapshot and conflict behavior

**Files:**
- Modify: `src/raftCore/raft.cpp`
- Create: `test/raft_append_entries_test.cpp`
- Modify: `test/CMakeLists.txt`

**Interfaces:**
```cpp
// Existing public test surface:
void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs*,
                          raftRpcProctoc::AppendEntriesReply*);
void Raft::Snapshot(int index, std::string snapshot);
```

- [ ] **Step 1: Write the failing snapshot-boundary test.** Build a single in-process Raft instance, commit entries, compact through index 2, then issue AppendEntries with `prevLogIndex` below the snapshot. Assert failure plus `updateNextIndex == snapshotIndex + 1`, and that the process returns normally.
- [ ] **Step 2: Write the failing conflict-suffix test.** Give the follower entries 1..4 in term 1, then send term-2 entries at indexes 2 and 3 after a matching predecessor at index 1. Assert the follower last index is 3, with term 2 at indexes 2 and 3: stale index 4 must be gone.
- [ ] **Step 3: Run the focused test and confirm RED.** Run `ctest --test-dir cmake-build-debug -R raft_append_entries_test --output-on-failure`; the legacy code must either abort at the snapshot assertion or retain the stale suffix.
- [ ] **Step 4: Return from the snapshot-rejection branch.** In `Raft::AppendEntries1`, set the existing failure response and return before `matchLog` can assert. Do not mutate logs or commit progress in this case.
- [ ] **Step 5: Implement first-conflict truncation.** For each existing incoming index: retain it when terms match; on the first term mismatch, erase the local element through the end, append this and remaining incoming entries, set persistent-state changed, and stop comparing old suffix entries. Preserve the existing invariant assertion for same index/term but divergent commands.
- [ ] **Step 6: Run focused tests GREEN.** Run `ctest --test-dir cmake-build-debug -R raft_append_entries_test --output-on-failure`.
- [ ] **Step 7: Run current Raft regressions.** Run `ctest --test-dir cmake-build-debug -L raft_integration --output-on-failure`.
- [ ] **Step 8: Commit.** Stage Raft code and test target; commit with `fix: handle snapshot boundary and truncate conflicting logs`.

## Task 4: Add opt-in deterministic Raft outbound fault policy

**Files:**
- Create: `src/raftCore/include/raft_fault_policy.h`
- Create: `src/raftCore/raft_fault_policy.cpp`
- Modify: `src/raftCore/include/raftRpcUtil.h`
- Modify: `src/raftCore/raftRpcUtil.cpp`
- Modify: `src/raftCore/include/kvServer.h`
- Modify: `src/raftCore/kvServer.cpp`
- Modify: `example/raftCoreExample/raftNode.cpp`
- Create: `test/raft_fault_policy_test.cpp`
- Modify: `test/CMakeLists.txt`

**Interfaces:**
```cpp
enum class RaftRpcMethod { AppendEntries, InstallSnapshot, RequestVote };
enum class RaftFaultAction { Allow, Drop, Delay, Duplicate };
struct RaftFaultDecision { RaftFaultAction action; std::chrono::milliseconds delay; };
RaftFaultDecision ReadRaftFaultPolicy(const std::filesystem::path&, int source,
                                      int target, RaftRpcMethod method);
RaftRpcUtil(std::string ip, uint16_t port, int sourceId, int targetId,
            std::filesystem::path faultPolicy = {});
KvServer(int me, int maxRaftState, std::filesystem::path configPath,
         std::filesystem::path dataDir, std::filesystem::path faultPolicy = {});
```

- [ ] **Step 1: Write the failing policy parser tests.** Exercise absent/empty policy (allow), exact and `any` method rules, drop, delay duration, duplicate, source/target filtering, and malformed input (fail closed to drop with an error log).
- [ ] **Step 2: Run the focused test and confirm RED.** Run `ctest --test-dir cmake-build-debug -R raft_fault_policy_test --output-on-failure`.
- [ ] **Step 3: Implement immutable-per-call policy lookup.** Read the plain-text policy file for each outbound call so tests can atomically replace it. Supported rule format is `source target method action [milliseconds]`; matching is deterministic and first-match wins.
- [ ] **Step 4: Apply actions in `RaftRpcUtil`.** Drop returns RPC failure without calling the stub; delay waits before the normal call; duplicate performs the normal call once into the official response then once into a temporary response, preserving the official result. Apply the existing per-peer `callMutex_` across the full fault action and call.
- [ ] **Step 5: Wire test-only configuration.** Add optional `--raft-fault-file` to `raftNode`; pass it through `KvServer` when constructing each peer utility. With no path, behavior must stay unchanged.
- [ ] **Step 6: Run focused tests GREEN.** Run `ctest --test-dir cmake-build-debug -R raft_fault_policy_test --output-on-failure`.
- [ ] **Step 7: Commit.** Stage the policy, Raft wiring, executable option, and test; commit with `test: add controllable Raft transport faults`.

## Task 5: Exercise fault recovery and concurrent client behavior end to end

**Files:**
- Modify: `test/integration/raft_cluster_test.py`
- Modify: `test/CMakeLists.txt`

- [ ] **Step 1: Extend `RaftCluster` test harness.** Create one policy file per temporary cluster, pass `--raft-fault-file` for every node, and provide atomic `set_fault_policy`/`clear_fault_policy` helpers using a replacement file. Add a leader wait helper that recognizes a new leader with a higher term while the old leader remains isolated.
- [ ] **Step 2: Write failing fault scenarios.** Cover (a) isolate the leader bidirectionally and elect a higher-term majority leader, (b) heal and verify the old leader catches up, (c) one-peer outbound delay while quorum commits continue, (d) one-peer drops while quorum commits continue, and (e) duplicate AppendEntries/RequestVote without divergent status or lost committed keys.
- [ ] **Step 3: Write the failing concurrent workload.** Use a fixed `random.Random(20260902)` seed and a bounded `ThreadPoolExecutor` to issue unique-key puts/gets from four clients. After convergence, assert every acknowledged value is readable from the cluster.
- [ ] **Step 4: Run the new integration test and confirm RED.** Run `ctest --test-dir cmake-build-debug -R raft_cluster_transport_faults --output-on-failure`; it should fail until policy plumbing/scenario support exists.
- [ ] **Step 5: Implement only harness-level support required by the tests.** Keep fault behavior in the policy file and RaftRpcUtil; do not add production-only test branches.
- [ ] **Step 6: Run all Raft integration tests GREEN.** Run `ctest --test-dir cmake-build-debug -L raft_integration --output-on-failure`.
- [ ] **Step 7: Run targeted native tests GREEN.** Run `ctest --test-dir cmake-build-debug -R '(rpc_frame_test|raft_append_entries_test|raft_fault_policy_test)' --output-on-failure`.
- [ ] **Step 8: Commit.** Stage integration changes; commit with `test: cover Raft transport faults and concurrent clients`.

## Final Verification

- [ ] Run `cmake --build cmake-build-debug --parallel`.
- [ ] Run `ctest --test-dir cmake-build-debug --output-on-failure`.
- [ ] Inspect `git diff --check` and confirm only the planned files changed.
- [ ] Record any test timeout or flaky behavior separately; do not weaken correctness assertions to hide it.
