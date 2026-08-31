#include "wal_format.h"

#include <limits>

namespace kvraft::wal {
namespace {

void PutU16(std::string& bytes, std::uint16_t value) {
  bytes.push_back(static_cast<char>((value >> 8) & 0xff));
  bytes.push_back(static_cast<char>(value & 0xff));
}

void PutU32(std::string& bytes, std::uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    bytes.push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

void PutU64(std::string& bytes, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    bytes.push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

std::uint16_t ReadU16(std::string_view bytes, std::size_t offset) {
  return (static_cast<std::uint16_t>(static_cast<unsigned char>(bytes[offset])) << 8) |
         static_cast<std::uint16_t>(static_cast<unsigned char>(bytes[offset + 1]));
}

std::uint32_t ReadU32(std::string_view bytes, std::size_t offset) {
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < 4; ++index) {
    value = (value << 8) | static_cast<unsigned char>(bytes[offset + index]);
  }
  return value;
}

std::uint64_t ReadU64(std::string_view bytes, std::size_t offset) {
  std::uint64_t value = 0;
  for (std::size_t index = 0; index < 8; ++index) {
    value = (value << 8) | static_cast<unsigned char>(bytes[offset + index]);
  }
  return value;
}

void ValidateHeader(std::string_view bytes, std::uint16_t* type, std::uint64_t* sequence,
                    std::uint32_t* raftLength, std::uint32_t* snapshotLength,
                    std::uint32_t* checksum) {
  if (bytes.size() < kHeaderSize) {
    throw PersistenceError("incomplete WAL header");
  }
  if (bytes.substr(0, kMagic.size()) != kMagic) {
    throw PersistenceError("invalid WAL magic");
  }
  if (ReadU16(bytes, 4) != kVersion) {
    throw PersistenceError("unsupported WAL version");
  }
  *type = ReadU16(bytes, 6);
  if (*type != static_cast<std::uint16_t>(RecordType::State) &&
      *type != static_cast<std::uint16_t>(RecordType::Snapshot)) {
    throw PersistenceError("invalid WAL record type");
  }
  *sequence = ReadU64(bytes, 8);
  *raftLength = ReadU32(bytes, 16);
  *snapshotLength = ReadU32(bytes, 20);
  *checksum = ReadU32(bytes, 24);
  if (static_cast<std::uint64_t>(*raftLength) + *snapshotLength > kMaxPayloadBytes) {
    throw PersistenceError("WAL payload too large");
  }
}

}  // namespace

std::uint32_t Crc32(std::string_view bytes) {
  std::uint32_t crc = 0xffffffffU;
  for (unsigned char byte : bytes) {
    crc ^= byte;
    for (int bit = 0; bit < 8; ++bit) {
      crc = (crc >> 1) ^ ((crc & 1U) == 0 ? 0U : 0xedb88320U);
    }
  }
  return crc ^ 0xffffffffU;
}

std::string Encode(const Record& record) {
  if (record.raftState.size() > kMaxPayloadBytes || record.snapshot.size() > kMaxPayloadBytes ||
      record.snapshot.size() > kMaxPayloadBytes - record.raftState.size() ||
      record.raftState.size() > std::numeric_limits<std::uint32_t>::max() ||
      record.snapshot.size() > std::numeric_limits<std::uint32_t>::max()) {
    throw PersistenceError("WAL payload too large");
  }
  const std::size_t payloadLength = record.raftState.size() + record.snapshot.size();
  if (record.type != RecordType::State && record.type != RecordType::Snapshot) {
    throw PersistenceError("invalid WAL record type");
  }

  std::string bytes;
  bytes.reserve(kHeaderSize + static_cast<std::size_t>(payloadLength));
  bytes.append(kMagic);
  PutU16(bytes, kVersion);
  PutU16(bytes, static_cast<std::uint16_t>(record.type));
  PutU64(bytes, record.sequence);
  PutU32(bytes, static_cast<std::uint32_t>(record.raftState.size()));
  PutU32(bytes, static_cast<std::uint32_t>(record.snapshot.size()));
  PutU32(bytes, 0);
  bytes.append(record.raftState);
  bytes.append(record.snapshot);

  std::string checksumInput = bytes.substr(4, 20);
  checksumInput.append(bytes, kHeaderSize, std::string::npos);
  const std::uint32_t checksum = Crc32(checksumInput);
  bytes[24] = static_cast<char>((checksum >> 24) & 0xff);
  bytes[25] = static_cast<char>((checksum >> 16) & 0xff);
  bytes[26] = static_cast<char>((checksum >> 8) & 0xff);
  bytes[27] = static_cast<char>(checksum & 0xff);
  return bytes;
}

Record Decode(std::string_view bytes) {
  std::uint16_t type = 0;
  std::uint64_t sequence = 0;
  std::uint32_t raftLength = 0;
  std::uint32_t snapshotLength = 0;
  std::uint32_t expectedChecksum = 0;
  ValidateHeader(bytes, &type, &sequence, &raftLength, &snapshotLength, &expectedChecksum);

  const std::size_t payloadLength = static_cast<std::size_t>(raftLength) + snapshotLength;
  if (bytes.size() != kHeaderSize + payloadLength) {
    throw PersistenceError("invalid WAL record length");
  }
  std::string checksumInput(bytes.substr(4, 20));
  checksumInput.append(bytes.substr(kHeaderSize));
  if (Crc32(checksumInput) != expectedChecksum) {
    throw PersistenceError("WAL checksum mismatch");
  }
  return {static_cast<RecordType>(type), sequence,
          std::string(bytes.substr(kHeaderSize, raftLength)),
          std::string(bytes.substr(kHeaderSize + raftLength, snapshotLength))};
}

}  // namespace kvraft::wal
