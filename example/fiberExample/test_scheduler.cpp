#include <atomic>

#include "monsoon.h"

const std::string LOG_HEAD = "[TASK] ";
std::atomic<int> completedTasks{0};
// 获取开始和结束线程的id
void test_fiber_1() {
  std::cout << LOG_HEAD << "tid = " << monsoon::GetThreadId() << ",test_fiber_1 begin" << std::endl;

  std::cout << LOG_HEAD << "tid = " << monsoon::GetThreadId() << ",test_fiber_1 finish" << std::endl;
  ++completedTasks;
}

void test_fiber_2() {
  std::cout << LOG_HEAD << "tid = " << monsoon::GetThreadId() << ",test_fiber_2 begin" << std::endl;

  sleep(3);// no hook 直接将当前协程阻塞，等效于将当前线程阻塞

  std::cout << LOG_HEAD << "tid = " << monsoon::GetThreadId() << ",test_fiber_2 finish" << std::endl;
  ++completedTasks;
}

void test_fiber_3() {
  std::cout << LOG_HEAD << "tid = " << monsoon::GetThreadId() << ",test_fiber_3 begin" << std::endl;

  std::cout << LOG_HEAD << "tid = " << monsoon::GetThreadId() << ",test_fiber_3 finish" << std::endl;
  ++completedTasks;
}

void test_fiber_4() {
  std::cout << LOG_HEAD << "tid = " << monsoon::GetThreadId() << ",test_fiber_4 begin" << std::endl;

  std::cout << LOG_HEAD << "tid = " << monsoon::GetThreadId() << ",test_fiber_4 finish" << std::endl;
  ++completedTasks;
}


// user_caller = true,使用main函数进行协程的调度，且单线程
void test_user_caller_1() {
  std::cout << "main begin" << std::endl;
  // 默认条件下 threads = 1,即只是用main函数进行协程的调度，等价于
  // 先攒下一些协程，然后切换到调度器的run方法执行这些协程，然后再返回
  // main函数
  monsoon::Scheduler sc;
  //将任务添加到调度器中，调度器会将这些任务放入任务队列中，等待调度器的调度
  sc.scheduler(test_fiber_1);
  sc.scheduler(test_fiber_2);

  monsoon::Fiber::ptr fiber(new monsoon::Fiber(&test_fiber_3));// 创建协程执行test_fiber_3函数
  sc.scheduler(fiber);
  // 创建调度线程，开始任务调度，如果只使用main函数线程进行调度，那start相当于什么也没做
  sc.start();
  sc.stop();
  std::cout << "main end" << std::endl;
}

// user_caller = true,使用main函数进行协程的调度，且多线程
bool test_user_caller_2() {
  completedTasks = 0;
  std::cout << "main begin" << std::endl;
  // main函数
  monsoon::Scheduler sc(3, true);

  sc.scheduler(test_fiber_1);
  sc.scheduler(test_fiber_2);

  monsoon::Fiber::ptr fiber(new monsoon::Fiber(&test_fiber_3));
  sc.scheduler(fiber);
  sc.start();
  sc.scheduler(test_fiber_4);
  sleep(5);
  /*
   * 停止调度，如果未使用当前线程（main）进行调度，那么只需要简单地等所有调度线程退出即可
   * 如果使用了当前线程进行调度，那么要先执行当前线程的协程调度函数，等其执行完后再返回caller协程继续往下执行
   */
  sc.stop();

  std::cout << "main end" << std::endl;
  return completedTasks == 4;
}

int main() {
  if (!test_user_caller_2()) {
    std::cerr << "scheduler did not complete all four tasks" << std::endl;
    return 1;
  }
  return 0;
}
