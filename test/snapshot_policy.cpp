#include "snapshot_policy.h"

#include <stdexcept>
#include <vector>

namespace {

void Expect(bool condition) {
  if (!condition) {
    throw std::runtime_error("snapshot policy result did not match the requested plan");
  }
}

}  // namespace

int main() {
  using kvraft::LogPosition;
  using kvraft::PlanSnapshotInstall;
  using kvraft::SnapshotDecision;

  try {
    const std::vector<LogPosition> logs{{11, 4}, {12, 4}, {13, 5}};
    const auto stale = PlanSnapshotInstall(10, 3, logs, 9, 2);
    Expect(stale.decision == SnapshotDecision::Stale);
    Expect(stale.firstRetainedLog == logs.size());

    const auto idempotent = PlanSnapshotInstall(10, 3, logs, 10, 3);
    Expect(idempotent.decision == SnapshotDecision::Idempotent);
    Expect(idempotent.firstRetainedLog == logs.size());

    const auto conflict = PlanSnapshotInstall(10, 3, logs, 10, 4);
    Expect(conflict.decision == SnapshotDecision::Conflict);
    Expect(conflict.firstRetainedLog == logs.size());

    const auto retain = PlanSnapshotInstall(10, 3, logs, 12, 4);
    Expect(retain.decision == SnapshotDecision::Install);
    Expect(retain.firstRetainedLog == 2);

    const auto discard = PlanSnapshotInstall(10, 3, logs, 12, 9);
    Expect(discard.decision == SnapshotDecision::Install);
    Expect(discard.firstRetainedLog == logs.size());
  } catch (const std::exception&) {
    return 1;
  }
  return 0;
}
