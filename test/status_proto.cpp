#include "kvServerRPC.pb.h"

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
