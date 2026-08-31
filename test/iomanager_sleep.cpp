#include <atomic>
#include <iostream>
#include <unistd.h>

#include "monsoon.h"

int main() {
  std::atomic<int> completionOrder{0};
  std::atomic<int> shortSleepOrder{0};
  std::atomic<int> longSleepOrder{0};

  {
    monsoon::IOManager ioManager(1, false);
    ioManager.scheduler([&] {
      usleep(200 * 1000);
      longSleepOrder = ++completionOrder;
    });
    ioManager.scheduler([&] {
      usleep(50 * 1000);
      shortSleepOrder = ++completionOrder;
    });
  }

  if (shortSleepOrder != 1 || longSleepOrder != 2) {
    std::cerr << "IOManager did not schedule the shorter cooperative sleep first" << std::endl;
    return 1;
  }
  return 0;
}
