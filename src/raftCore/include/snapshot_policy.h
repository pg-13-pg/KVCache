#ifndef SNAPSHOT_POLICY_H
#define SNAPSHOT_POLICY_H

#include <cstddef>
#include <span>

namespace kvraft {

enum class SnapshotDecision { Stale, Idempotent, Conflict, Install };

struct LogPosition {
  int index;
  int term;
};

struct SnapshotInstallPlan {
  SnapshotDecision decision;
  std::size_t firstRetainedLog;
};

SnapshotInstallPlan PlanSnapshotInstall(int localSnapshotIndex, int localSnapshotTerm,
                                        std::span<const LogPosition> logs, int incomingIndex,
                                        int incomingTerm);

}  // namespace kvraft

#endif  // SNAPSHOT_POLICY_H
