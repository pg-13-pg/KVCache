# Raft Correctness Baseline Design

Date: 2026-08-31

## 1. Purpose

The first modernization phase turns the existing Raft KV demonstration into a
repeatable correctness baseline. It must prove that committed data survives
leader failure, process restart, log compaction, and snapshot installation.

This phase uses real Linux processes, TCP connections, and files. It does not
attempt deterministic packet loss or clock simulation yet.

## 2. Existing Problems Addressed

The current launcher forks all nodes from one process and lets each RPC server
append its address to a shared configuration file. This makes individual node
lifecycle control and deterministic test setup difficult.

The current persister truncates its files during construction, uses formatted
text stream extraction for serialized payloads, and does not make Raft state
and snapshot updates crash-safe as a pair. A process restart therefore does
not currently provide a reliable durability test.

The current automated tests cover utility and fiber behavior but do not cover
Raft election, replication, failover, full-cluster restart, or snapshot catchup.
`CondInstallSnapshot` also accepts every snapshot without checking freshness.

## 3. Scope

This phase includes:

- a one-process-per-node executable;
- static cluster configuration and per-node data directories;
- a read-only node status RPC;
- bounded command-line client operations for test orchestration;
- an append-only, checksummed Raft WAL with atomic snapshot compaction;
- restart recovery and current-term no-op commitment;
- stale snapshot rejection and ordered state-machine installation;
- black-box multi-process integration tests registered with CTest.

This phase does not include:

- packet loss, delay, duplication, or network partition injection;
- a complete RPC framing and backpressure redesign;
- ReadIndex or lease reads;
- membership changes, sharding, authentication, or a web UI;
- an LSM-tree storage engine;
- cross-platform process-failure tests outside Linux and WSL.

## 4. Architecture

CTest invokes a Python standard-library test controller. The controller creates
an isolated artifact directory, writes a complete cluster configuration, starts
three `raftNode` processes, invokes `kvctl`, queries status, and controls process
lifecycle with signals.

```text
CTest
  `-- test/integration/raft_cluster_test.py
        |-- cluster configuration
        |-- node-0 process ---- data/node-0/raft.wal
        |-- node-1 process ---- data/node-1/raft.wal
        |-- node-2 process ---- data/node-2/raft.wal
        `-- kvctl put|get|status
```

The test controller is orchestration only. Raft, RPC, persistence, snapshots,
and KV behavior remain implemented in C++.

### 4.1 `raftNode`

`raftNode` runs exactly one `KvServer` and one `Raft` instance. Its command line
is:

```text
raftNode --id N --config PATH --data-dir PATH --max-raft-state BYTES
```

The configuration file is complete before any node starts. Nodes must never
modify it. Tests use loopback addresses and one unique TCP port per node.

The process validates its node id, configuration, writable data directory, and
port before starting election timers. Startup failure is reported to stderr and
returns a non-zero exit status.

The existing `raftCoreRun` launcher remains available as a convenience wrapper.
It will generate configuration first and then start `raftNode` children with
explicit data directories. It is not used by integration tests.

### 4.2 `kvctl`

`kvctl` provides bounded operations:

```text
kvctl --config PATH --timeout-ms N put KEY VALUE
kvctl --config PATH --timeout-ms N get KEY
kvctl --config PATH --timeout-ms N status --node N
```

`put` exits zero only after the operation is committed. `get` writes only the
value to stdout and exits zero when the key exists. `status` emits one stable
line containing `node_id`, `term`, `role`, `commit_index`, `last_applied`, and
`snapshot_index`. Failures go to stderr and use a non-zero exit status.

The existing `Clerk` API remains compatible. New bounded methods take an
absolute deadline and return an explicit result instead of retrying forever.
`kvctl` uses only the bounded methods.

### 4.3 Status RPC

The KV service protobuf gains a read-only `GetStatus` method. It directly reads
a mutex-protected Raft status snapshot and does not enter the replicated log.
The response contains:

- node id;
- current term;
- role (`FOLLOWER`, `CANDIDATE`, or `LEADER`);
- commit index;
- last applied index;
- last included snapshot index and term.

The endpoint is diagnostic, not a consistency-bearing user operation. Tests
must not infer KV correctness from status alone; user data is verified through
normal committed client reads.

## 5. Durable Storage

Each node stores one append-only file at `<data-dir>/raft.wal`. All fields in
the fixed header use big-endian byte order and fixed-width integers.

```text
RecordHeader
  magic:            4 bytes ("KVRW")
  version:          uint16 (1)
  record_type:      uint16 (STATE=1, SNAPSHOT=2)
  sequence:         uint64
  raft_state_len:   uint32
  snapshot_len:     uint32
  checksum:         uint32

Payload
  raft_state:       raft_state_len bytes
  snapshot:         snapshot_len bytes
```

CRC32 covers the header fields from `version` through `snapshot_len`, followed
by both payloads. `magic` and the checksum field itself are excluded. Lengths
are validated against the remaining file size and a fixed maximum payload size
before allocation.

Boost serialization can remain the initial Raft payload encoding, but it is
treated as binary data. Persistence must not use formatted stream insertion or
extraction.

### 5.1 State Records

`SaveRaftState` appends one `STATE` record containing the full current persistent
Raft state and an empty snapshot payload. The implementation holds the persister
mutex, performs complete-write loops that handle `EINTR` and short writes, then
calls `fdatasync` before reporting success.

The state considered persistent by Raft is `currentTerm`, `votedFor`, the log,
and snapshot boundary metadata. A vote or RPC response that depends on a state
change must not be sent before the corresponding persistence call succeeds.

### 5.2 Snapshot Records And Compaction

A `SNAPSHOT` record contains the matching Raft state and KV snapshot in the same
record. This makes the pair transactional during recovery.

After a snapshot is built, compaction writes a temporary WAL containing one
complete `SNAPSHOT` record, calls `fdatasync`, renames it over `raft.wal`, and
fsyncs the parent directory. The append descriptor is reopened only after the
rename succeeds. All steps occur under the persister mutex.

Subsequent state changes append `STATE` records. The last valid state record is
the recovered Raft state; the snapshot payload comes from the last valid
snapshot record.

### 5.3 Recovery

Recovery scans from the beginning and accepts only records with valid magic,
version, type, monotonic sequence, lengths, and CRC32.

- EOF in a partial header or payload is treated as a torn tail. The tail is
  ignored and truncated before new records are appended.
- A checksum mismatch in a complete record or corruption before the final tail
  is a startup error. The node must not silently continue with uncertain state.
- An absent WAL means a new node. Construction never truncates an existing WAL.
- Legacy text persistence files are not migrated automatically. The launcher
  uses a new explicit data directory so legacy files cannot be mistaken for the
  new format.

Persistence errors throw a storage-specific exception. Continuing without a
durable term, vote, or log entry could violate Raft safety, so the node logs the
error and exits non-zero instead of serving further requests.

`RaftStateSize` reports the current serialized Raft state size, not total WAL
file size, so the existing snapshot threshold keeps its intended meaning.

## 6. Raft And Snapshot Recovery

Node startup follows this order:

1. Open and recover the WAL.
2. Restore Raft term, vote, log, and snapshot boundary.
3. Restore the KV state-machine snapshot and client deduplication table.
4. Start RPC service and background election/application tasks.

When a candidate becomes leader, it appends an internal no-op entry for its
current term. Committing that entry allows a restarted cluster to establish and
apply all preceding committed entries without waiting for a user write. The KV
state machine recognizes and ignores the no-op command while still advancing
its applied index.

Snapshot installation obeys these rules:

- a snapshot below the local snapshot index is rejected as stale;
- equal index and equal term is idempotent;
- equal index with a different term is a consistency error;
- a newer snapshot truncates only log entries it covers;
- snapshot boundary, retained log, and snapshot payload are persisted together
  before success is reported;
- commit index and last applied index are updated in memory after persistence;
  recovery initializes both volatile indices to at least the snapshot index;
- the KV state machine installs snapshots only in apply-channel order and only
  when newer than its own installed snapshot index.

`CondInstallSnapshot` will implement these checks rather than returning an
unconditional result. Apply messages remain ordered so a later log cannot be
applied before the snapshot on which it depends.

## 7. Integration Test Harness

The controller uses `subprocess.Popen(..., start_new_session=True)` so each node
has a controllable process group. It reserves three loopback ports, writes the
configuration, closes the reservation sockets, and starts nodes immediately.
Port allocation is retried if startup reports an address collision.

Artifacts live below the CTest binary directory. Successful test artifacts are
removed. Failed test artifacts are retained and their absolute path is printed.
Each node has separate stdout/stderr logs.

All polling uses bounded deadlines and short backoff. Cleanup sends `SIGTERM`,
waits for a bounded grace period, and then sends `SIGKILL`. Cleanup runs in a
`finally` block and verifies that no controlled process remains.

### 7.1 Scenarios

1. **Election**: start three nodes and observe exactly one leader. Observed
   leaders are tracked by term; two different leaders in one observed term fail
   the test.
2. **Replication**: write 100 distinct keys, read each value, and wait for all
   nodes' `last_applied` values to reach the committed position.
3. **Leader failure**: kill the observed leader with `SIGKILL`, wait for one of
   the remaining nodes to lead a higher term, then read all committed values.
4. **Follower catchup**: continue writes while the old leader is down, restart
   it with the same data directory, and wait for its commit and applied indices
   to catch up.
5. **Full-cluster restart**: commit values, kill every node with `SIGKILL`,
   restart all data directories, wait for election, and verify every value.
6. **Snapshot recovery**: configure a small state threshold, write until a
   positive snapshot index is observed, restart the cluster, and verify data.
7. **InstallSnapshot catchup**: stop a follower, write until the leader has
   compacted past the follower's last index, restart the follower, and require
   its snapshot and applied indices to catch up before verifying cluster data.
8. **Torn WAL tail**: stop a node, append a deliberately incomplete record,
   restart it, and verify recovery from the preceding valid record.

No scenario uses status fields as the sole data-integrity assertion. Values are
always checked with normal client reads after the cluster has a quorum.

### 7.2 Unit Tests

Focused persister tests cover:

- payloads containing spaces, newlines, and NUL bytes;
- close and reopen recovery;
- partial header and partial payload tails;
- checksum corruption in a complete record;
- snapshot compaction followed by later state records;
- monotonic sequence validation;
- independent per-node data directories.

Focused snapshot tests cover stale, idempotent, conflicting, and newer snapshot
decisions and verify retained-log boundaries.

## 8. CMake And CTest Integration

The new executables use normal target-level include paths and link dependencies.
Python is discovered with CMake's `find_package(Python3 COMPONENTS Interpreter)`.
If Python is absent, configuration reports that Raft integration tests are
unavailable; unit tests and normal builds remain available.

Integration tests carry the `raft_integration` label and explicit timeouts.
Linux and WSL run the full suite. Unsupported platforms register the tests as
skipped rather than passed.

Primary verification commands are:

```bash
cmake --build cmake-build-debug -j6
ctest --test-dir cmake-build-debug --output-on-failure
ctest --test-dir cmake-build-debug -L raft_integration --repeat until-fail:10 --output-on-failure
```

## 9. Acceptance Criteria

The phase is complete when:

- all existing unit and example tests still pass;
- all persistence and snapshot unit tests pass;
- every integration scenario passes;
- the integration label passes ten consecutive runs without timeout;
- no test leaves controlled node processes running;
- failed scenarios retain enough logs and status history for diagnosis;
- manual one-node-per-process startup and the compatibility launcher are
  documented;
- a full-cluster `SIGKILL` and restart preserves every acknowledged write;
- a follower whose required logs were compacted catches up through
  `InstallSnapshot`.

## 10. Follow-up Phases

After this baseline is stable, the next independent phases are:

1. length-delimited RPC request and response framing, partial IO handling,
   deadlines, cancellation, and backpressure;
2. deterministic in-process transport fault injection for partitions, delay,
   drops, duplication, and seeded randomized schedules;
3. ReadIndex linearizable reads with quorum confirmation and an applied-index
   barrier;
4. benchmarks, metrics, sanitizer jobs, and CI reporting.
