#include "Persister.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <sys/stat.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace {

constexpr std::size_t kWalCompactionFloorBytes = 1024 * 1024;
constexpr std::size_t kWalLiveStateMultiplier = 8;

std::size_t WalCompactionLimit(std::size_t raftStateBytes, std::size_t snapshotBytes) {
  const std::size_t checkpointBytes = kvraft::wal::kHeaderSize + raftStateBytes + snapshotBytes;
  if (checkpointBytes > std::numeric_limits<std::size_t>::max() / kWalLiveStateMultiplier) {
    return std::numeric_limits<std::size_t>::max();
  }
  return std::max(kWalCompactionFloorBytes, checkpointBytes * kWalLiveStateMultiplier);
}

[[noreturn]] void ThrowSystemError(const std::string& operation, const fs::path& path) {
  throw kvraft::PersistenceError(operation + " " + path.string() + ": " + std::strerror(errno));
}

void WriteFully(int fd, std::string_view bytes, const fs::path& path) {
  std::size_t written = 0;
  while (written < bytes.size()) {
    const ssize_t result = write(fd, bytes.data() + written, bytes.size() - written);
    if (result < 0) {
      if (errno == EINTR) {
        continue;
      }
      ThrowSystemError("write failed for", path);
    }
    if (result == 0) {
      throw kvraft::PersistenceError("short write to " + path.string());
    }
    written += static_cast<std::size_t>(result);
  }
}

void ReadFully(int fd, char* destination, std::size_t size, off_t offset, const fs::path& path) {
  std::size_t readBytes = 0;
  while (readBytes < size) {
    const ssize_t result = pread(fd, destination + readBytes, size - readBytes, offset + readBytes);
    if (result < 0) {
      if (errno == EINTR) {
        continue;
      }
      ThrowSystemError("read failed for", path);
    }
    if (result == 0) {
      throw kvraft::PersistenceError("unexpected end of WAL " + path.string());
    }
    readBytes += static_cast<std::size_t>(result);
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

void ValidateCompleteHeader(std::string_view header) {
  if (header.substr(0, kvraft::wal::kMagic.size()) != kvraft::wal::kMagic) {
    throw kvraft::PersistenceError("invalid WAL magic");
  }
  if (ReadU16(header, 4) != kvraft::wal::kVersion) {
    throw kvraft::PersistenceError("unsupported WAL version");
  }
  const std::uint16_t type = ReadU16(header, 6);
  if (type != static_cast<std::uint16_t>(kvraft::wal::RecordType::State) &&
      type != static_cast<std::uint16_t>(kvraft::wal::RecordType::Snapshot)) {
    throw kvraft::PersistenceError("invalid WAL record type");
  }
  if (static_cast<std::uint64_t>(ReadU32(header, 16)) + ReadU32(header, 20) >
      kvraft::wal::kMaxPayloadBytes) {
    throw kvraft::PersistenceError("WAL payload too large");
  }
}

std::size_t PayloadSize(std::string_view header) {
  return static_cast<std::size_t>(ReadU32(header, 16)) + ReadU32(header, 20);
}

void SyncDirectory(const fs::path& directory) {
  const int directoryFd = open(directory.c_str(), O_RDONLY | O_DIRECTORY);
  if (directoryFd < 0) {
    ThrowSystemError("open failed for directory", directory);
  }
  if (fsync(directoryFd) != 0) {
    const int savedErrno = errno;
    close(directoryFd);
    errno = savedErrno;
    ThrowSystemError("fsync failed for directory", directory);
  }
  if (close(directoryFd) != 0) {
    ThrowSystemError("close failed for directory", directory);
  }
}

void CloseOrThrow(int fd, const fs::path& path) {
  if (close(fd) != 0) {
    ThrowSystemError("close failed for", path);
  }
}

}  // namespace

Persister::Persister(fs::path dataDir) : dataDir_(std::move(dataDir)), walPath_(dataDir_ / "raft.wal") {
  std::error_code error;
  fs::create_directories(dataDir_, error);
  if (error) {
    throw kvraft::PersistenceError("failed to create WAL directory " + dataDir_.string() + ": " +
                                   error.message());
  }
  Recover();
}

Persister::Persister(int me)
    : Persister(fs::path("data") / ("node-" + std::to_string(me))) {}

Persister::~Persister() {
  if (walFd_ >= 0) {
    close(walFd_);
  }
}

void Persister::Save(std::string raftState, std::string snapshot) {
  std::lock_guard<std::mutex> lock(mutex_);
  CompactSnapshot(raftState, snapshot);
}

void Persister::SaveRaftState(const std::string& data) {
  std::lock_guard<std::mutex> lock(mutex_);
  const std::size_t nextRecordBytes = kvraft::wal::kHeaderSize + data.size();
  const std::size_t compactionLimit = WalCompactionLimit(data.size(), snapshot_.size());
  if (walBytes_ > compactionLimit || nextRecordBytes > compactionLimit - walBytes_) {
    CompactSnapshot(data, snapshot_);
  } else {
    AppendRecord(kvraft::wal::RecordType::State, data, "");
  }
}

std::string Persister::ReadSnapshot() {
  std::lock_guard<std::mutex> lock(mutex_);
  return snapshot_;
}

std::string Persister::ReadRaftState() {
  std::lock_guard<std::mutex> lock(mutex_);
  return raftState_;
}

long long Persister::RaftStateSize() {
  std::lock_guard<std::mutex> lock(mutex_);
  return static_cast<long long>(raftState_.size());
}

const fs::path& Persister::WalPath() const noexcept { return walPath_; }

void Persister::Recover() {
  const bool existed = fs::exists(walPath_);
  walFd_ = open(walPath_.c_str(), O_RDWR | O_CREAT, 0644);
  if (walFd_ < 0) {
    ThrowSystemError("open failed for", walPath_);
  }
  if (!existed) {
    SyncDirectory(dataDir_);
  }

  struct stat status {};
  if (fstat(walFd_, &status) != 0) {
    ThrowSystemError("fstat failed for", walPath_);
  }
  if (status.st_size < 0) {
    throw kvraft::PersistenceError("invalid WAL size " + walPath_.string());
  }

  const off_t fileSize = status.st_size;
  off_t offset = 0;
  off_t lastValidOffset = 0;
  std::uint64_t lastSequence = 0;
  bool sawRecord = false;
  while (offset < fileSize) {
    const off_t remaining = fileSize - offset;
    if (remaining < static_cast<off_t>(kvraft::wal::kHeaderSize)) {
      break;
    }

    std::string header(kvraft::wal::kHeaderSize, '\0');
    ReadFully(walFd_, header.data(), header.size(), offset, walPath_);
    ValidateCompleteHeader(header);
    const std::size_t payloadSize = PayloadSize(header);
    if (payloadSize > static_cast<std::size_t>(fileSize - offset - kvraft::wal::kHeaderSize)) {
      break;
    }

    std::string bytes = header;
    bytes.resize(kvraft::wal::kHeaderSize + payloadSize);
    if (payloadSize > 0) {
      ReadFully(walFd_, bytes.data() + kvraft::wal::kHeaderSize, payloadSize,
                offset + static_cast<off_t>(kvraft::wal::kHeaderSize), walPath_);
    }
    const kvraft::wal::Record record = kvraft::wal::Decode(bytes);
    if (sawRecord && record.sequence <= lastSequence) {
      throw kvraft::PersistenceError("non-monotonic WAL sequence");
    }
    sawRecord = true;
    lastSequence = record.sequence;
    if (record.type == kvraft::wal::RecordType::State) {
      raftState_ = record.raftState;
    } else {
      raftState_ = record.raftState;
      snapshot_ = record.snapshot;
    }
    offset += static_cast<off_t>(bytes.size());
    lastValidOffset = offset;
  }

  if (lastValidOffset != fileSize) {
    if (ftruncate(walFd_, lastValidOffset) != 0) {
      ThrowSystemError("ftruncate failed for", walPath_);
    }
    if (fdatasync(walFd_) != 0) {
      ThrowSystemError("fdatasync failed for", walPath_);
    }
  }
  if (sawRecord) {
    if (lastSequence == std::numeric_limits<std::uint64_t>::max()) {
      throw kvraft::PersistenceError("WAL sequence exhausted");
    }
    nextSequence_ = lastSequence + 1;
  }
  walBytes_ = static_cast<std::size_t>(lastValidOffset);

  if (lseek(walFd_, 0, SEEK_END) < 0) {
    ThrowSystemError("seek failed for", walPath_);
  }

  const fs::path temporaryPath = walPath_.string() + ".tmp";
  std::error_code error;
  const bool temporaryExists = fs::exists(temporaryPath, error);
  if (error) {
    throw kvraft::PersistenceError("failed to inspect temporary WAL " + temporaryPath.string() + ": " +
                                   error.message());
  }
  if (temporaryExists) {
    fs::remove(temporaryPath, error);
    if (error) {
      throw kvraft::PersistenceError("failed to remove temporary WAL " + temporaryPath.string() + ": " +
                                     error.message());
    }
    SyncDirectory(dataDir_);
  }
}

void Persister::AppendRecord(kvraft::wal::RecordType type, const std::string& raftState,
                             const std::string& snapshot) {
  const std::string encoded = kvraft::wal::Encode({type, nextSequence_, raftState, snapshot});
  WriteFully(walFd_, encoded, walPath_);
  if (fdatasync(walFd_) != 0) {
    ThrowSystemError("fdatasync failed for", walPath_);
  }
  if (nextSequence_ == std::numeric_limits<std::uint64_t>::max()) {
    throw kvraft::PersistenceError("WAL sequence exhausted");
  }
  ++nextSequence_;
  walBytes_ += encoded.size();
  if (type == kvraft::wal::RecordType::State) {
    raftState_ = raftState;
  } else {
    raftState_ = raftState;
    snapshot_ = snapshot;
  }
}

void Persister::CompactSnapshot(const std::string& raftState, const std::string& snapshot) {
  const fs::path temporaryPath = walPath_.string() + ".tmp";
  const std::string encoded =
      kvraft::wal::Encode({kvraft::wal::RecordType::Snapshot, nextSequence_, raftState, snapshot});
  const int temporaryFd = open(temporaryPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (temporaryFd < 0) {
    ThrowSystemError("open failed for", temporaryPath);
  }
  WriteFully(temporaryFd, encoded, temporaryPath);
  if (fdatasync(temporaryFd) != 0) {
    const int savedErrno = errno;
    close(temporaryFd);
    errno = savedErrno;
    ThrowSystemError("fdatasync failed for", temporaryPath);
  }
  CloseOrThrow(temporaryFd, temporaryPath);

  CloseOrThrow(walFd_, walPath_);
  walFd_ = -1;
  if (rename(temporaryPath.c_str(), walPath_.c_str()) != 0) {
    ThrowSystemError("rename failed for", walPath_);
  }
  SyncDirectory(dataDir_);
  walFd_ = open(walPath_.c_str(), O_RDWR);
  if (walFd_ < 0) {
    ThrowSystemError("open failed for", walPath_);
  }
  if (lseek(walFd_, 0, SEEK_END) < 0) {
    ThrowSystemError("seek failed for", walPath_);
  }
  if (nextSequence_ == std::numeric_limits<std::uint64_t>::max()) {
    throw kvraft::PersistenceError("WAL sequence exhausted");
  }
  ++nextSequence_;
  walBytes_ = encoded.size();
  raftState_ = raftState;
  snapshot_ = snapshot;
}
