#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>

#include "skipList.h"

#include <stdexcept>
#include <string>

namespace {

class ThrowOnCopyValue {
 public:
  ThrowOnCopyValue() = default;
  explicit ThrowOnCopyValue(std::string value) : value_(std::move(value)) {}

  ThrowOnCopyValue(const ThrowOnCopyValue& other) : value_(other.value_) { ThrowIfEnabled(); }

  ThrowOnCopyValue& operator=(const ThrowOnCopyValue& other) {
    ThrowIfEnabled();
    value_ = other.value_;
    return *this;
  }

  ThrowOnCopyValue(ThrowOnCopyValue&&) noexcept = default;
  ThrowOnCopyValue& operator=(ThrowOnCopyValue&&) noexcept = default;

  const std::string& value() const { return value_; }

  template <class Archive>
  void serialize(Archive& archive, const unsigned int) {
    archive& value_;
  }

  static bool throwOnCopy;

 private:
  static void ThrowIfEnabled() {
    if (throwOnCopy) {
      throw std::runtime_error("injected value copy failure");
    }
  }

  std::string value_;
};

bool ThrowOnCopyValue::throwOnCopy = false;

void Expect(bool condition, const char* message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

std::string MakeMismatchedDump() {
  SkipListDump<std::string, std::string> dump;
  dump.keyDumpVt_.push_back("replacement");

  std::stringstream stream;
  boost::archive::text_oarchive archive(stream);
  archive << dump;
  return stream.str();
}

void TestRestoreReplacesLiveState() {
  SkipList<std::string, std::string> source(6);
  std::string alpha = "alpha";
  std::string value = "value-not-equal-to-key";
  source.insert_set_element(alpha, value);
  const std::string dump = source.dump_file();

  SkipList<std::string, std::string> restored(6);
  std::string stale = "stale";
  std::string staleValue = "remove-me";
  restored.insert_set_element(stale, staleValue);
  restored.load_file(dump);

  std::string actual;
  Expect(restored.search_element(alpha, actual) && actual == value,
         "restore did not retain the serialized value");
  Expect(!restored.search_element(stale, actual), "restore did not remove stale state");
}

void TestMalformedRestoreLeavesLiveStateUntouched() {
  SkipList<std::string, std::string> restored(6);
  std::string stable = "stable";
  std::string stableValue = "keep-me";
  restored.insert_set_element(stable, stableValue);

  bool rejected = false;
  try {
    restored.load_file(MakeMismatchedDump());
  } catch (const std::runtime_error&) {
    rejected = true;
  }

  std::string actual;
  Expect(rejected, "malformed snapshot was accepted");
  Expect(restored.search_element(stable, actual) && actual == stableValue,
         "malformed restore changed live state");
  Expect(restored.size() == 1, "malformed restore changed live entry count");
}

void TestCopyFailureLeavesLiveStateUntouched() {
  SkipList<std::string, ThrowOnCopyValue> source(6);
  std::string replacement = "replacement";
  ThrowOnCopyValue replacementValue("new-value");
  source.insert_set_element(replacement, replacementValue);
  const std::string dump = source.dump_file();

  SkipList<std::string, ThrowOnCopyValue> restored(6);
  std::string stable = "stable";
  ThrowOnCopyValue stableValue("keep-me");
  restored.insert_set_element(stable, stableValue);

  bool rejected = false;
  ThrowOnCopyValue::throwOnCopy = true;
  try {
    restored.load_file(dump);
  } catch (const std::runtime_error&) {
    rejected = true;
  }
  ThrowOnCopyValue::throwOnCopy = false;

  ThrowOnCopyValue actual;
  Expect(rejected, "restore accepted an injected node-copy failure");
  Expect(restored.search_element(stable, actual) && actual.value() == "keep-me",
         "copy failure changed live state");
  Expect(restored.size() == 1, "copy failure changed live entry count");
}

}  // namespace

int main() {
  try {
    TestRestoreReplacesLiveState();
    TestMalformedRestoreLeavesLiveStateUntouched();
    TestCopyFailureLeavesLiveStateUntouched();
  } catch (const std::exception&) {
    return 1;
  }
  return 0;
}
