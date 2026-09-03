#pragma once

#include <chrono>
#include <filesystem>

enum class RaftRpcMethod { AppendEntries, InstallSnapshot, RequestVote };
enum class RaftFaultAction { Allow, Drop, Delay, Duplicate };

struct RaftFaultDecision {
  RaftFaultAction action = RaftFaultAction::Allow;
  std::chrono::milliseconds delay{0};
};

// Reads the current policy file. An absent or empty file permits the call.
// Any malformed policy fails closed and returns Drop.
RaftFaultDecision ReadRaftFaultPolicy(const std::filesystem::path& path, int source, int target,
                                      RaftRpcMethod method);

