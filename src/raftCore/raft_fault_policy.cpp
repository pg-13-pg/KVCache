#include "raft_fault_policy.h"

#include <charconv>
#include <cctype>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

std::string Trim(std::string_view value) {
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) value.remove_prefix(1);
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back()))) value.remove_suffix(1);
  return std::string(value);
}

bool MatchesId(std::string_view token, int id) {
  if (token == "any") return true;
  int parsed = 0;
  const auto result = std::from_chars(token.data(), token.data() + token.size(), parsed);
  return !token.empty() && result.ec == std::errc{} && result.ptr == token.data() + token.size() && parsed >= 0 && parsed == id;
}

std::string MethodName(RaftRpcMethod method) {
  switch (method) {
    case RaftRpcMethod::AppendEntries: return "AppendEntries";
    case RaftRpcMethod::InstallSnapshot: return "InstallSnapshot";
    case RaftRpcMethod::RequestVote: return "RequestVote";
  }
  return {};
}

bool MatchesMethod(std::string_view token, RaftRpcMethod method) {
  return token == "any" || token == MethodName(method);
}

bool ParseNonNegativeMilliseconds(std::string_view token, std::chrono::milliseconds* value) {
  long long parsed = 0;
  const auto result = std::from_chars(token.data(), token.data() + token.size(), parsed);
  if (token.empty() || result.ec != std::errc{} || result.ptr != token.data() + token.size() || parsed < 0) {
    return false;
  }
  if (parsed > std::chrono::milliseconds::max().count()) return false;
  *value = std::chrono::milliseconds(parsed);
  return true;
}

RaftFaultDecision Malformed(const std::filesystem::path& path, std::size_t lineNumber, const std::string& reason) {
  std::cerr << "invalid Raft fault policy " << path << " line " << lineNumber << ": " << reason << '\n';
  return {RaftFaultAction::Drop, std::chrono::milliseconds(0)};
}

struct CachedPolicyFile {
  std::filesystem::file_time_type modified;
  std::uintmax_t size = 0;
  std::string contents;
};

std::mutex& PolicyCacheMutex() {
  static std::mutex mutex;
  return mutex;
}

std::unordered_map<std::string, CachedPolicyFile>& PolicyCache() {
  static std::unordered_map<std::string, CachedPolicyFile> cache;
  return cache;
}

}  // namespace

RaftFaultDecision ReadRaftFaultPolicy(const std::filesystem::path& path, int source, int target,
                                      RaftRpcMethod method) {
  if (path.empty()) return {};
  std::error_code statusError;
  if (!std::filesystem::exists(path, statusError)) {
    std::lock_guard<std::mutex> lock(PolicyCacheMutex());
    PolicyCache().erase(path.string());
    return {};
  }

  std::error_code metadataError;
  const auto modified = std::filesystem::last_write_time(path, metadataError);
  const auto size = std::filesystem::file_size(path, metadataError);
  std::string contents;
  bool cacheHit = false;
  {
    std::lock_guard<std::mutex> lock(PolicyCacheMutex());
    const auto it = PolicyCache().find(path.string());
    if (!metadataError && it != PolicyCache().end() && it->second.modified == modified && it->second.size == size) {
      contents = it->second.contents;
      cacheHit = true;
    }
  }
  if (!cacheHit) {
    std::ifstream input(path);
    if (!input) return Malformed(path, 0, "cannot read policy file");
    contents.assign(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    if (!metadataError) {
      std::lock_guard<std::mutex> lock(PolicyCacheMutex());
      PolicyCache()[path.string()] = CachedPolicyFile{modified, size, contents};
    }
  } else if (contents.empty() && size == 0) {
    std::ifstream input(path);
    if (!input) return Malformed(path, 0, "cannot read policy file");
    if (!metadataError) {
      std::lock_guard<std::mutex> lock(PolicyCacheMutex());
      PolicyCache()[path.string()] = CachedPolicyFile{modified, size, {}};
    }
  }

  std::istringstream input(contents);
  std::string line;
  std::size_t lineNumber = 0;
  while (std::getline(input, line)) {
    ++lineNumber;
    const auto comment = line.find('#');
    if (comment != std::string::npos) line.resize(comment);
    const std::string trimmed = Trim(line);
    if (trimmed.empty()) continue;

    std::istringstream tokens(trimmed);
    std::vector<std::string> fields;
    for (std::string token; tokens >> token;) fields.push_back(std::move(token));
    if (fields.size() < 4 || fields.size() > 5) return Malformed(path, lineNumber, "expected source target method action [milliseconds]");

    const std::string& sourceToken = fields[0];
    const std::string& targetToken = fields[1];
    const std::string& methodToken = fields[2];
    const std::string& actionToken = fields[3];
    if (!MatchesId(sourceToken, source) || !MatchesId(targetToken, target) || !MatchesMethod(methodToken, method)) {
      // Validate every rule even when it does not match, so malformed files
      // cannot silently alter behavior depending on the current RPC.
      const auto idOk = [](std::string_view token) {
        if (token == "any") return true;
        int parsed = 0;
        const auto result = std::from_chars(token.data(), token.data() + token.size(), parsed);
        return !token.empty() && result.ec == std::errc{} && result.ptr == token.data() + token.size() && parsed >= 0;
      };
      if (!idOk(sourceToken) || !idOk(targetToken) || !(methodToken == "any" || methodToken == "AppendEntries" ||
                                                        methodToken == "InstallSnapshot" || methodToken == "RequestVote")) {
        return Malformed(path, lineNumber, "invalid source, target, or method");
      }
      if (actionToken != "drop" && actionToken != "delay" && actionToken != "duplicate") {
        return Malformed(path, lineNumber, "unknown action");
      }
      if ((actionToken == "delay" && fields.size() != 5) || (actionToken != "delay" && fields.size() != 4)) {
        return Malformed(path, lineNumber, "invalid action arguments");
      }
      if (actionToken == "delay") {
        std::chrono::milliseconds ignoredDelay;
        if (!ParseNonNegativeMilliseconds(fields[4], &ignoredDelay)) return Malformed(path, lineNumber, "invalid delay");
      }
      continue;
    }

    if (actionToken == "drop" && fields.size() == 4) return {RaftFaultAction::Drop, std::chrono::milliseconds(0)};
    if (actionToken == "duplicate" && fields.size() == 4) return {RaftFaultAction::Duplicate, std::chrono::milliseconds(0)};
    if (actionToken == "delay" && fields.size() == 5) {
      std::chrono::milliseconds delay;
      if (ParseNonNegativeMilliseconds(fields[4], &delay)) return {RaftFaultAction::Delay, delay};
      return Malformed(path, lineNumber, "invalid delay");
    }
    return Malformed(path, lineNumber, "invalid action arguments");
  }
  return {};
}
