#include "scheduler.hpp"
#include "fiber.hpp"
#include "hook.hpp"


namespace monsoon {
// 当前线程所属的调度器，同一调度器下的所有线程共享同一调度器实例 （线程级调度器）
static thread_local Scheduler *cur_scheduler = nullptr;
// 当前线程的调度协程，每个线程一个 (协程级调度器)
static thread_local Fiber *cur_scheduler_fiber = nullptr;
const std::string LOG_HEAD = "[scheduler] ";

//根据 use_caller 决定是否让当前线程参与调度；如果参与，就先创建当前线程的主协程，再创建一个运行 Scheduler::run() 
//的调度协程 rootFiber_，并把当前线程记录为调度线程；剩余需要创建的工作线程数保存在 threadCnt_。
Scheduler::Scheduler(size_t threads, bool use_caller, const std::string &name) {
  CondPanic(threads > 0, "threads <= 0");

  isUseCaller_ = use_caller;
  name_ = name;
  ////caller线程：主协程 + 协程调度器协程 + 任务协程
  // use_caller:是否将当前线程也作为被调度线程
  if (use_caller) {
    std::cout << LOG_HEAD << "current thread as called thread" << std::endl;
    // 总线程数减1
    --threads;
    // 初始化caller线程的主协程，线程原始上下文，主协程不参与调度器调度
    Fiber::GetThis();
    std::cout << LOG_HEAD << "init caller thread's main fiber success" << std::endl;
    CondPanic(GetThis() == nullptr, "GetThis err:cur scheduler is not nullptr");
    // 设置当前线程为调度器线程（caller thread）
    cur_scheduler = this;
    // 初始化当前线程的调度协程 （该线程不会被调度器调用），调度结束后，返回主协程
    rootFiber_.reset(new Fiber(std::bind(&Scheduler::run, this), 0, false));
    std::cout << LOG_HEAD << "init caller thread's caller fiber success" << std::endl;

    Thread::SetName(name_);
    cur_scheduler_fiber = rootFiber_.get();
    rootThread_ = GetThreadId();
    threadIds_.push_back(rootThread_);
  } else {
    rootThread_ = -1;
  }
  threadCnt_ = threads;
  std::cout << "-------scheduler init success-------" << std::endl;
}

Scheduler *Scheduler::GetThis() { return cur_scheduler; }

Fiber *Scheduler::GetMainFiber() { return cur_scheduler_fiber; }

void Scheduler::setThis() { cur_scheduler = this; }

Scheduler::~Scheduler() {
  CondPanic(isStopped_, "isstopped is false");
  if (GetThis() == this) {
    cur_scheduler = nullptr;
  }
}

// 调度器启动，初始化调度线程池，start() 只启动额外工作线程（Scheduler::run）
void Scheduler::start() {
  std::cout << LOG_HEAD << "scheduler start" << std::endl;
  Mutex::Lock lock(mutex_);
  if (isStopped_) {
    std::cout << "scheduler has stopped" << std::endl;
    return;
  }
  CondPanic(threadPool_.empty(), "thread pool is not empty");
  threadPool_.resize(threadCnt_);
  for (size_t i = 0; i < threadCnt_; i++) {
    threadPool_[i].reset(new Thread(std::bind(&Scheduler::run, this), name_ + "_" + std::to_string(i)));
    threadIds_.push_back(threadPool_[i]->getId());
  }
}

//创建调度器线程池后，Scheduler::run() 负责从任务队列中取出任务并执行，直到调度器停止。
void Scheduler::run() {
  std::cout << LOG_HEAD << "begin run" << std::endl;
  set_hook_enable(true);  //使用hook，hook的系统调用会让出协程执行权，等待IO事件触发后再恢复执行
  setThis();
  if (GetThreadId() != rootThread_) {
    // 如果当前线程不是caller线程，则初始化该线程的调度协程
    cur_scheduler_fiber = Fiber::GetThis().get();
  }

  // 创建idle协程
  Fiber::ptr idleFiber(new Fiber(std::bind(&Scheduler::idle, this)));

  Fiber::ptr cbFiber;  // 用于执行cb回调函数任务的协程
  SchedulerTask task;
  while (true) {
    task.reset();
    // 是否通知其他线程进行任务调度
    bool tickle_me = false;
    {
      Mutex::Lock lock(mutex_);
      auto it = tasks_.begin();  //遍历任务队列，找到一个可调度的任务
      while (it != tasks_.end()) {
        // 发现已经指定调度线程，但不是在当前线程进行调度，需要通知其他线程进行调度，并跳过当前任务
        if (it->thread_ != -1 && it->thread_ != GetThreadId()) {
          ++it;
          tickle_me = true;
          continue;
        }
        CondPanic(it->fiber_ || it->cb_, "task is nullptr");
        if (it->fiber_) {
          CondPanic(it->fiber_->getState() == Fiber::READY, "fiber task state error");
        }
        // 找到一个可进行任务，准备开始调度，从任务队列取出，活动线程加1
        task = *it;
        tasks_.erase(it++);
        ++activeThreadCnt_;
        break;
      }
      // 当前线程拿出一个任务后，同时任务队列不空，那么告诉其他线程
      tickle_me |= (it != tasks_.end());
    }

    if (tickle_me) {
      tickle();
    }

    if (task.fiber_) {// 任务是协程对象
      // 开始执行 协程任务
      task.fiber_->resume();
      // 执行结束
      --activeThreadCnt_;
      task.reset();
    } else if (task.cb_) {  //任务是函数回调对象
      if (cbFiber) {
        cbFiber->reset(task.cb_);
      } else {
        cbFiber.reset(new Fiber(task.cb_));
      }
      task.reset();
      cbFiber->resume();
      --activeThreadCnt_;
      cbFiber.reset();
    } else {   // 没有任务，执行idle协程
      if (idleFiber->getState() == Fiber::TERM) { //idle协程结束，说明调度器已经停止
        std::cout << "idle fiber term" << std::endl;
        break;
      }
      // idle协程不断空轮转，不断地resume和yield（Scheduler::idle），等待调度器调度
      ++idleThreadCnt_;
      idleFiber->resume();
      --idleThreadCnt_;
    }
  }
  std::cout << "run exit" << std::endl;
}
   
void Scheduler::tickle() { std::cout << "tickle" << std::endl; }  //现在无实际作用

// 判断是否可以停止
bool Scheduler::stopping() {
  Mutex::Lock lock(mutex_);
  return isStopped_ && tasks_.empty() && activeThreadCnt_ == 0;
}

// idle协程，当前线程没有任务时，执行idle协程，等待调度器调度
void Scheduler::idle() {
  while (!stopping()) {
    Fiber::GetThis()->yield();// idle协程不断让出执行权，等待调度器调度
  }
}

// stop() 停止调度器。
// use_caller = true：resume rootFiber_，让 caller 线程进入 run() 处理剩余任务并退出。
// use_caller = false：工作线程已在 start() 中运行 run()，这里只唤醒并等待它们退出。
void Scheduler::stop() {
  std::cout << LOG_HEAD << "stop" << std::endl;
  if (stopping()) {
    return;
  }
  isStopped_ = true;

  // stop指令只能由caller线程发起
  if (isUseCaller_) {
    CondPanic(GetThis() == this, "cur thread is not caller thread");
  } else {
    CondPanic(GetThis() != this, "cur thread is caller thread");
  }

  for (size_t i = 0; i < threadCnt_; i++) {
    tickle();
  }
  if (rootFiber_) {
    tickle();
  }

  if (rootFiber_) {
    // 切换到调度协程，开始调度
    rootFiber_->resume();
    std::cout << "root fiber end" << std::endl;
  }

  std::vector<Thread::ptr> threads;
  {
    Mutex::Lock lock(mutex_);
    threads.swap(threadPool_);
  }
  for (auto &i : threads) { // 等待所有线程结束
    i->join();
  }
}

}  // namespace monsoon

