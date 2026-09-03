# Raft Transport Hardening Design

Date: 2026-09-02

## Purpose

Make the Raft KV cluster correct under normal TCP stream behavior, large RPC
payloads, overlapping client calls, log conflicts, and test-controlled Raft
transport faults. This is a coordinated upgrade: old and new RPC wire formats
do not interoperate.

## Scope

This change includes:

- a length-delimited request and response protocol with complete-I/O handling;
- a per-channel transaction mutex around connection, send, receive, and
  reconnect operations;
- correct AppendEntries snapshot-boundary and conflicting-suffix behavior;
- opt-in, test-only Raft RPC fault injection for drop, delay, and duplication;
- unit and integration coverage for the repaired behavior, large values,
  partitions, and concurrent clients.

It does not add request multiplexing, rolling wire-protocol upgrades,
production fault injection controls, ReadIndex, or membership changes.

## RPC Wire Protocol

Every request and every response is exactly one frame:

```text
uint32_be payload_length
payload[payload_length]
```

Request payloads preserve the existing protobuf encoding:

```text
varint32 rpc_header_length
serialized RpcHeader
serialized request protobuf
```

Response payloads are the serialized response protobuf. The implementation
defines one explicit maximum frame length shared by client and server. A zero
length response is valid for an empty protobuf; a request must contain a valid
header and arguments. A length above the maximum or a malformed payload closes
the connection without dispatching a service method.

### Server Behavior

`RpcProvider::OnMessage` leaves incomplete data in Muduo's `Buffer`. It loops
only while the buffer contains a full 4-byte length header and the declared
payload. Each loop iteration extracts exactly one frame and dispatches one RPC.
Thus partial TCP packets wait for the next callback and coalesced packets are
processed as independent requests.

`SendRpcResponse` prepends the response frame length before calling
`TcpConnection::send`.

### Client Behavior

`MprpcChannel::CallMethod` serializes a request frame, then holds a channel
mutex across the following complete transaction:

```text
connect or reconnect -> send all request bytes -> read 4-byte length ->
validate length -> read all response bytes -> parse response
```

Short sends and reads retry until the requested byte count is complete. EINTR
retries; timeouts, EOF, malformed data, and other socket failures close the
descriptor while holding the same mutex, set `m_clientFd` to `-1`, and fail the
controller. The next caller can reconnect safely.

One `MprpcChannel` has at most one in-flight RPC. This makes FIFO request and
response matching sufficient and deliberately avoids request IDs and a response
dispatcher. `RaftRpcUtil` keeps its existing non-blocking per-peer lock so
periodic Raft work does not pile up stale requests; the channel mutex is the
general safety boundary used by every RPC caller.

## Raft AppendEntries Behavior

When a follower receives AppendEntries with a term that it can process:

1. If `prevLogIndex` is beyond its last log index, it returns failure with the
   next retry index as today.
2. If `prevLogIndex` is below the local snapshot index, it returns failure with
   `updateNextIndex = snapshotIndex + 1` and returns immediately. It must not
   call `matchLog` with an index outside its retained log range.
3. If the predecessor term does not match, it returns the existing conflict-term
   backtracking hint without changing the log.
4. If the predecessor matches, entries are examined in order. Existing entries
   with equal index and term are retained after command equality is checked.
5. On the first existing entry with a different term, the follower deletes that
   entry and every later retained entry, appends the received entry and every
   later incoming entry, marks persistent state dirty, and ends the scan.
6. Entries after the local tail are appended normally.

A conflicting entry at or below `commitIndex` is rejected without mutation.
That state should be unreachable in a correct Raft execution, but preserving
committed state is safer than overwriting it when an invariant is violated.

The follower persists once after a successful log mutation, updates its commit
index only after its log has been reconciled, and sends a success response only
for the completed request.

## Test-Only Raft Fault Injection

Fault injection belongs at the outbound `RaftRpcUtil` boundary, not in Raft's
state machine or the generic RPC transport. It is disabled unless `raftNode`
receives `--raft-fault-file PATH`.

Each utility instance is constructed with immutable source and target node IDs.
Before a Raft RPC is sent, it reads the current fault policy from that path. The
integration controller updates the policy by writing a replacement file and
atomically renaming it into place, so readers observe either the old complete
policy or the new complete policy.

The line-oriented policy format is:

```text
# source target method action [argument]
0 1 any drop
1 2 AppendEntries delay 75
2 0 RequestVote duplicate
```

`source` and `target` are decimal node IDs. `method` is `any`,
`AppendEntries`, `InstallSnapshot`, or `RequestVote`.

- `drop` skips the RPC and reports transport failure to its caller.
- `delay N` sleeps for `N` milliseconds before performing the normal RPC.
- `duplicate` performs the same RPC twice in sequence and returns the first
  invocation's result and response to Raft; the second invocation exists only
  to verify receiver idempotence.

Malformed policies fail closed for the matching call: the RPC is skipped and a
diagnostic is logged. An absent or empty policy permits all calls. This feature
is only exercised by tests and is not exposed by the normal launcher or client.

## Verification

Focused C++ tests cover:

- a follower whose snapshot boundary is ahead of `prevLogIndex` replies failure
  without terminating;
- conflicting AppendEntries deletes the entire suffix and retains matching
  entries;
- equal term but unequal command remains an invariant failure;
- frame helpers accept split and concatenated frames and reject oversize frames.

Python integration scenarios cover:

- a value larger than the old 1024-byte receive buffer survives write, read,
  replication, and restart;
- a bidirectional partition isolates the old leader, produces a higher-term
  replacement leader, and converges after the policy is cleared;
- bounded delay, drop, and duplicate policies permit continued quorum progress
  and convergence after removal;
- deterministic, seeded concurrent `kvctl` writers create distinct keys and
  every acknowledged value is readable after all live nodes catch up.

All existing unit and Raft integration tests remain passing. The new fault
scenarios run with bounded deadlines and retain their artifacts on failure.
