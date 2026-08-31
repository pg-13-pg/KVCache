#include "snapshot_policy.h"

namespace kvraft {

SnapshotInstallPlan PlanSnapshotInstall(int localSnapshotIndex, int localSnapshotTerm,
                                        std::span<const LogPosition> logs, int incomingIndex,
                                        int incomingTerm) {
  if (incomingIndex < localSnapshotIndex) {
    return {SnapshotDecision::Stale, logs.size()};
  }
  if (incomingIndex == localSnapshotIndex) {
    return {incomingTerm == localSnapshotTerm ? SnapshotDecision::Idempotent
                                              : SnapshotDecision::Conflict,
            logs.size()};
  }

  for (std::size_t index = 0; index < logs.size(); ++index) {
    if (logs[index].index == incomingIndex && logs[index].term == incomingTerm) {
      return {SnapshotDecision::Install, index + 1};
    }
  }
  return {SnapshotDecision::Install, logs.size()};
}

}  // namespace kvraft
