#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>

#include "skipList.h"

#include <string>

int main() {
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
  if (!restored.search_element(alpha, actual) || actual != value) return 1;
  if (restored.search_element(stale, actual)) return 2;
  return 0;
}
