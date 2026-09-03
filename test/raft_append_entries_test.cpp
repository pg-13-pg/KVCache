#include "raft.h"

#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

std::shared_ptr<Raft> MakeRaft(const std::filesystem::path& data_dir) {
  auto raft = std::make_shared<Raft>();
  auto persister = std::make_shared<Persister>(data_dir);
  auto apply_channel = std::make_shared<LockQueue<ApplyMsg>>();
  raft->init({}, 0, std::move(persister), std::move(apply_channel));
  return raft;
}

raftRpcProctoc::LogEntry Entry(int index, int term, const std::string& command) {
  raftRpcProctoc::LogEntry entry;
  entry.set_logindex(index);
  entry.set_logterm(term);
  entry.set_command(command);
  return entry;
}

void AppendInitialEntries(Raft& raft, int leader_commit = 2) {
  raftRpcProctoc::AppendEntriesArgs args;
  args.set_term(1);
  args.set_leaderid(1);
  args.set_prevlogindex(0);
  args.set_prevlogterm(0);
  args.set_leadercommit(leader_commit);
  for (int index = 1; index <= 4; ++index) {
    *args.add_entries() = Entry(index, 1, "term1-" + std::to_string(index));
  }

  raftRpcProctoc::AppendEntriesReply reply;
  raft.AppendEntries1(&args, &reply);
  Expect(reply.success(), "initial AppendEntries was rejected");
  Expect(raft.getLastLogIndex() == 4, "initial entries were not appended");
}

void TestSnapshotBoundaryRejectsOldAppendEntries() {
  const auto data_dir = std::filesystem::temp_directory_path() / "kvraft-append-snapshot-boundary";
  std::filesystem::remove_all(data_dir);
  auto raft = MakeRaft(data_dir);
  AppendInitialEntries(*raft, 2);

  raft->Snapshot(2, "snapshot-at-2");
  const auto before = raft->GetStatusSnapshot();
  Expect(before.snapshotIndex == 2, "snapshot did not advance its index");
  Expect(raft->getLastLogIndex() == 4, "snapshot discarded the live suffix");

  raftRpcProctoc::AppendEntriesArgs stale;
  stale.set_term(1);
  stale.set_leaderid(1);
  stale.set_prevlogindex(1);
  stale.set_prevlogterm(1);
  stale.set_leadercommit(4);
  *stale.add_entries() = Entry(2, 1, "term1-2");
  raftRpcProctoc::AppendEntriesReply reply;
  raft->AppendEntries1(&stale, &reply);

  Expect(!reply.success(), "AppendEntries before snapshot was accepted");
  Expect(reply.updatenextindex() == 3, "snapshot boundary returned the wrong next index");
  const auto after = raft->GetStatusSnapshot();
  Expect(after.snapshotIndex == before.snapshotIndex, "rejected AppendEntries changed snapshot state");
  Expect(after.commitIndex == before.commitIndex, "rejected AppendEntries changed commit state");
  Expect(raft->getLastLogIndex() == 4, "rejected AppendEntries changed log state");

  std::filesystem::remove_all(data_dir);
}

void TestCommittedConflictIsRejected() {
  const auto data_dir = std::filesystem::temp_directory_path() / "kvraft-append-committed-conflict";
  std::filesystem::remove_all(data_dir);
  auto raft = MakeRaft(data_dir);
  AppendInitialEntries(*raft, 4);

  raftRpcProctoc::AppendEntriesArgs replacement;
  replacement.set_term(2);
  replacement.set_leaderid(1);
  replacement.set_prevlogindex(1);
  replacement.set_prevlogterm(1);
  replacement.set_leadercommit(4);
  *replacement.add_entries() = Entry(2, 2, "committed-replacement");
  raftRpcProctoc::AppendEntriesReply reply;
  raft->AppendEntries1(&replacement, &reply);

  Expect(!reply.success(), "conflict below commit index was accepted");
  Expect(raft->getLastLogIndex() == 4, "committed conflict changed log length");
  Expect(raft->getLogTermFromLogIndex(2) == 1, "committed conflict changed log term");
  Expect(raft->GetStatusSnapshot().commitIndex == 4, "committed conflict changed commit index");
  std::filesystem::remove_all(data_dir);
}

void TestAppendEntriesTruncatesConflictingSuffix() {
  const auto data_dir = std::filesystem::temp_directory_path() / "kvraft-append-conflict-suffix";
  std::filesystem::remove_all(data_dir);
  auto raft = MakeRaft(data_dir);
  AppendInitialEntries(*raft, 0);

  raftRpcProctoc::AppendEntriesArgs replacement;
  replacement.set_term(2);
  replacement.set_leaderid(1);
  replacement.set_prevlogindex(1);
  replacement.set_prevlogterm(1);
  replacement.set_leadercommit(3);
  *replacement.add_entries() = Entry(2, 2, "term2-2");
  *replacement.add_entries() = Entry(3, 2, "term2-3");
  raftRpcProctoc::AppendEntriesReply reply;
  raft->AppendEntries1(&replacement, &reply);

  Expect(reply.success(), "conflicting AppendEntries was rejected");
  Expect(raft->getLastLogIndex() == 3, "stale suffix survived a conflicting AppendEntries");
  Expect(raft->getLogTermFromLogIndex(2) == 2, "index 2 retained the old term");
  Expect(raft->getLogTermFromLogIndex(3) == 2, "index 3 retained the old term");

  std::filesystem::remove_all(data_dir);
}

void TestAppendEntriesRejectsNonContiguousEntries() {
  const auto data_dir = std::filesystem::temp_directory_path() / "kvraft-append-invalid-gap";
  std::filesystem::remove_all(data_dir);
  auto raft = MakeRaft(data_dir);
  AppendInitialEntries(*raft, 0);

  raftRpcProctoc::AppendEntriesArgs malformed;
  malformed.set_term(2);
  malformed.set_leaderid(1);
  malformed.set_prevlogindex(1);
  malformed.set_prevlogterm(1);
  *malformed.add_entries() = Entry(3, 2, "skipped-index");
  raftRpcProctoc::AppendEntriesReply reply;
  raft->AppendEntries1(&malformed, &reply);

  Expect(!reply.success(), "non-contiguous AppendEntries was accepted");
  Expect(raft->getLastLogIndex() == 4, "malformed AppendEntries changed log state");
  std::filesystem::remove_all(data_dir);
}

}  // namespace

int main() {
  try {
    TestSnapshotBoundaryRejectsOldAppendEntries();
    TestAppendEntriesTruncatesConflictingSuffix();
    TestCommittedConflictIsRejected();
    TestAppendEntriesRejectsNonContiguousEntries();
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
  return 0;
}
