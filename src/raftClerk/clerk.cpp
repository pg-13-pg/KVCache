#include "clerk.h"
#include "cluster_config.h"
#include "raftServerRpcUtil.h"
#include "util.h"
#include <algorithm>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

namespace {

void SleepAfterCycle(std::chrono::steady_clock::time_point deadline) {
  const auto wake = std::chrono::steady_clock::now() + std::chrono::milliseconds(20);
  if (deadline == std::chrono::steady_clock::time_point::max()) {
    std::this_thread::sleep_until(wake);
  } else {
    std::this_thread::sleep_until(std::min(wake, deadline));
  }
}

}  // namespace

ClerkStatus Clerk::GetUntil(const std::string& key, std::string* value,
                            std::chrono::steady_clock::time_point deadline) {
  if (m_servers.empty()) return ClerkStatus::Unavailable;
  value->clear();
  m_requestId++;
  const auto requestId = m_requestId;
  std::size_t server = static_cast<std::size_t>(m_recentLeaderId) % m_servers.size();
  std::size_t attempts = 0;
  raftKVRpcProctoc::GetArgs args;
  args.set_key(key);
  args.set_clientid(m_clientId);
  args.set_requestid(requestId);

  while (true) {
    if (std::chrono::steady_clock::now() >= deadline) return ClerkStatus::TimedOut;
    raftKVRpcProctoc::GetReply reply;
    const bool rpcOk = m_servers[server]->Get(&args, &reply);
    if (rpcOk && reply.err() == ErrNoKey) return ClerkStatus::NotFound;
    if (rpcOk && reply.err() == OK) {
      m_recentLeaderId = static_cast<int>(server);
      *value = reply.value();
      return ClerkStatus::Ok;
    }
    server = (server + 1) % m_servers.size();
    if (++attempts % m_servers.size() == 0) SleepAfterCycle(deadline);
  }
}

ClerkStatus Clerk::PutAppendUntil(const std::string& key, const std::string& value,
                                  const std::string& op,
                                  std::chrono::steady_clock::time_point deadline) {
  if (m_servers.empty()) return ClerkStatus::Unavailable;
  m_requestId++;
  const auto requestId = m_requestId;
  std::size_t server = static_cast<std::size_t>(m_recentLeaderId) % m_servers.size();
  std::size_t attempts = 0;
  raftKVRpcProctoc::PutAppendArgs args;
  args.set_key(key);
  args.set_value(value);
  args.set_op(op);
  args.set_clientid(m_clientId);
  args.set_requestid(requestId);
  while (true) {
    if (std::chrono::steady_clock::now() >= deadline) return ClerkStatus::TimedOut;
    raftKVRpcProctoc::PutAppendReply reply;
    const bool rpcOk = m_servers[server]->PutAppend(&args, &reply);
    if (rpcOk && reply.err() == OK) {
      m_recentLeaderId = static_cast<int>(server);
      return ClerkStatus::Ok;
    }
    server = (server + 1) % m_servers.size();
    if (++attempts % m_servers.size() == 0) SleepAfterCycle(deadline);
  }
}

std::string Clerk::Get(std::string key) {
  std::string value;
  (void)GetUntil(key, &value, std::chrono::steady_clock::time_point::max());
  return value;
}

void Clerk::Put(std::string key, std::string value) {
  (void)PutUntil(key, value, std::chrono::steady_clock::time_point::max());
}

void Clerk::Append(std::string key, std::string value) {
  (void)AppendUntil(key, value, std::chrono::steady_clock::time_point::max());
}

ClerkStatus Clerk::PutUntil(const std::string& key, const std::string& value,
                            std::chrono::steady_clock::time_point deadline) {
  return PutAppendUntil(key, value, "Put", deadline);
}

ClerkStatus Clerk::AppendUntil(const std::string& key, const std::string& value,
                               std::chrono::steady_clock::time_point deadline) {
  return PutAppendUntil(key, value, "Append", deadline);
}

//初始化客户端
void Clerk::Init(std::string configFileName, std::chrono::milliseconds ioTimeout) {
  m_servers.clear();
  for (const auto& endpoint : LoadClusterConfig(configFileName)) {
    m_servers.push_back(std::make_shared<raftServerRpcUtil>(endpoint.ip, endpoint.port, ioTimeout));
  }
}

Clerk::Clerk() : m_clientId(Uuid()), m_requestId(0), m_recentLeaderId(0) {}
