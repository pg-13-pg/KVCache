#include "../src/common/include/util.h"

void myAssert(bool condition, std::string message) {
  if (!condition) {
    std::cerr << "Error: " << message << std::endl;
    std::exit(EXIT_FAILURE);
  }
}

int main() {
  const std::string expected =
      "[func-AppendEntries-rf{1}] 两节点logIndex{2}和term{3}相同，但是其command{4:5}    {6:7}却不同！！\n";
  const std::string actual =
      format("[func-AppendEntries-rf{%d}] 两节点logIndex{%d}和term{%d}相同，但是其command{%d:%d}   "
             " {%d:%d}却不同！！\n",
             1, 2, 3, 4, 5, 6, 7);
  myAssert(actual == expected, "format() returned an unexpected string: " + actual);
  return 0;
}
