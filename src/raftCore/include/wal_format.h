#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

namespace kvraft {

class PersistenceError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

namespace wal {

inline constexpr std::string_view kMagic = "KVRW";
inline constexpr std::uint16_t kVersion = 1;
inline constexpr std::size_t kHeaderSize = 28;
inline constexpr std::uint64_t kMaxPayloadBytes = 1ULL << 30;

enum class RecordType : std::uint16_t { State = 1, Snapshot = 2 };

struct Record {
  RecordType type;
  std::uint64_t sequence;
  std::string raftState;
  std::string snapshot;
};

std::uint32_t Crc32(std::string_view bytes);
std::string Encode(const Record& record);
Record Decode(std::string_view bytes);

}  // namespace wal
}  // namespace kvraft
