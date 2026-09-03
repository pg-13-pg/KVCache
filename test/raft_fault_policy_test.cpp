#include "raft_fault_policy.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>

namespace fs = std::filesystem;

namespace {

void Expect(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}

fs::path Write(const fs::path& path, const std::string& contents) {
  std::ofstream output(path);
  output << contents;
  return path;
}

}  // namespace

int main() {
  const auto root = fs::temp_directory_path() /
                    ("kvraft-fault-policy-" + std::to_string(
                        std::chrono::steady_clock::now().time_since_epoch().count()));
  fs::create_directories(root);
  try {
    const auto absent = root / "absent";
    Expect(ReadRaftFaultPolicy(absent, 0, 1, RaftRpcMethod::AppendEntries).action == RaftFaultAction::Allow,
           "absent policy should allow");

    const auto empty = Write(root / "empty", "\n# comment\n");
    Expect(ReadRaftFaultPolicy(empty, 0, 1, RaftRpcMethod::AppendEntries).action == RaftFaultAction::Allow,
           "empty policy should allow");

    const auto rules = Write(root / "rules",
        "0 1 AppendEntries drop\n"
        "1 2 any delay 37\n"
        "2 0 RequestVote duplicate\n"
        "any 4 InstallSnapshot drop\n");
    auto decision = ReadRaftFaultPolicy(rules, 0, 1, RaftRpcMethod::AppendEntries);
    Expect(decision.action == RaftFaultAction::Drop, "exact drop rule did not match");
    decision = ReadRaftFaultPolicy(rules, 1, 2, RaftRpcMethod::RequestVote);
    Expect(decision.action == RaftFaultAction::Delay && decision.delay == std::chrono::milliseconds(37),
           "any-method delay rule did not match");
    decision = ReadRaftFaultPolicy(rules, 2, 0, RaftRpcMethod::RequestVote);
    Expect(decision.action == RaftFaultAction::Duplicate, "duplicate rule did not match");
    decision = ReadRaftFaultPolicy(rules, 3, 4, RaftRpcMethod::InstallSnapshot);
    Expect(decision.action == RaftFaultAction::Drop, "source wildcard rule did not match");
    Expect(ReadRaftFaultPolicy(rules, 0, 2, RaftRpcMethod::AppendEntries).action == RaftFaultAction::Allow,
           "source/target filtering ignored");

    const auto firstMatch = Write(root / "first-match", "0 1 any drop\n0 1 AppendEntries delay 5\n");
    Expect(ReadRaftFaultPolicy(firstMatch, 0, 1, RaftRpcMethod::AppendEntries).action == RaftFaultAction::Drop,
           "first matching rule was not selected");

    const auto malformed = Write(root / "malformed", "0 1 AppendEntries delay nope\n");
    Expect(ReadRaftFaultPolicy(malformed, 0, 1, RaftRpcMethod::AppendEntries).action == RaftFaultAction::Drop,
           "malformed matching policy should fail closed");
    const auto malformedAction = Write(root / "malformed-action", "0 1 AppendEntries unknown\n");
    Expect(ReadRaftFaultPolicy(malformedAction, 3, 4, RaftRpcMethod::RequestVote).action == RaftFaultAction::Drop,
           "malformed nonmatching policy should fail closed");
    const auto malformedArgs = Write(root / "malformed-args", "0 1 AppendEntries drop 1\n");
    Expect(ReadRaftFaultPolicy(malformedArgs, 3, 4, RaftRpcMethod::RequestVote).action == RaftFaultAction::Drop,
           "invalid action arguments should fail closed");

    const auto hotReload = Write(root / "hot-reload", "0 1 AppendEntries drop\n");
    Expect(ReadRaftFaultPolicy(hotReload, 0, 1, RaftRpcMethod::AppendEntries).action == RaftFaultAction::Drop,
           "initial hot-reload policy did not match");
    Write(hotReload, "0 1 AppendEntries delay 3\n");
    decision = ReadRaftFaultPolicy(hotReload, 0, 1, RaftRpcMethod::AppendEntries);
    Expect(decision.action == RaftFaultAction::Delay && decision.delay == std::chrono::milliseconds(3),
           "policy cache did not observe file replacement");
  } catch (...) {
    fs::remove_all(root);
    return 1;
  }
  fs::remove_all(root);
  return 0;
}
