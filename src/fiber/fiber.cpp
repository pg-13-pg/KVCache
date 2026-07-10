#include "fiber.hpp"
#include <assert.h>
#include <atomic>
#include "scheduler.hpp"
#include "utils.hpp"

namespace monsoon {
const bool DEBUG = true;
// 当前线程正在运行的协程
static thread_local Fiber *cur_fiber = nullptr;  //thread_local 表示每个线程都有自己独立的一份 cur_fiber。
// 当前线程的主协程
static thread_local Fiber::ptr cur_thread_fiber = nullptr;
// 用于生成协程Id
static std::atomic<uint64_t> cur_fiber_id{0};
// 统计当前协程数
static std::atomic<uint64_t> fiber_count{0};
// 协议栈默认大小 128k
static int g_fiber_stack_size = 128 * 1024;

//栈分配器（堆区）
class StackAllocator {
 public:
  static void *Alloc(size_t size) { return malloc(size); }
  static void Delete(void *vp, size_t size) { return free(vp); }
};

// only for GetThis  第一次创建协程并设置为主协程，主协程使用线程的栈空间，不需要分配新的栈空间
Fiber::Fiber() {
  SetThis(this);  //设置当前线程的协程为主协程
  state_ = RUNNING;
  CondPanic(getcontext(&ctx_) == 0, "getcontext error");//保存当前线程的上下文到ctx_中
  ++fiber_count;
  id_ = cur_fiber_id++;
  std::cout << "[fiber] create fiber , id = " << id_ << std::endl;
}

// 设置当前协程
void Fiber::SetThis(Fiber *f) { cur_fiber = f; }

// 获取当前执行协程，不存在则创建
Fiber::ptr Fiber::GetThis() {
  if (cur_fiber) {
    return cur_fiber->shared_from_this();//cur_fiber 指向的那个Fiber对象，调用 shared_from_this()，返回管理这个Fiber对象的 shared_ptr。
  }
  // 创建主协程并初始化 
  Fiber::ptr main_fiber(new Fiber);  //创建一个Fiber对象，并且 shared_ptr 管理主协程对象
  CondPanic(cur_fiber == main_fiber.get(), "cur_fiber need to be main_fiber");
  cur_thread_fiber = main_fiber;
  return cur_fiber->shared_from_this();
}

// 有参构造，并为新的子协程创建栈空间
Fiber::Fiber(std::function<void()> cb, size_t stacksize, bool run_inscheduler)
    : id_(cur_fiber_id++), cb_(cb), isRunInScheduler_(run_inscheduler) {
  ++fiber_count;
  stackSize_ = stacksize > 0 ? stacksize : g_fiber_stack_size;
  stack_ptr = StackAllocator::Alloc(stackSize_);
  CondPanic(getcontext(&ctx_) == 0, "getcontext error");
  // 初始化协程上下文
  ctx_.uc_link = nullptr;  //当前上下文函数执行完后，自动切到哪个上下文。
  ctx_.uc_stack.ss_sp = stack_ptr;
  ctx_.uc_stack.ss_size = stackSize_;
  makecontext(&ctx_, &Fiber::MainFunc, 0);//当以后切换到这个上下文 ctx_ 时，从 Fiber::MainFunc() 开始执行。

}

// 切换当前协程到执行态,并保存主协程的上下文
void Fiber::resume() {
  CondPanic(state_ != TERM && state_ != RUNNING, "state error");  //resume 只能在 READY 状态下调用
  SetThis(this);
  state_ = RUNNING;

  if (isRunInScheduler_) {
    // 当前协程参与调度器调度，则与调度器主协程进行swap
    CondPanic(0 == swapcontext(&(Scheduler::GetMainFiber()->ctx_), &ctx_),
              "isRunInScheduler_ = true,swapcontext error");
  } else {
    // 切换主协程到当前协程，并保存主协程上下文到子协程ctx_
    CondPanic(0 == swapcontext(&(cur_thread_fiber->ctx_), &ctx_), "isRunInScheduler_ = false,swapcontext error");
  }
}

// 当前协程让出执行权
// 协程执行完成之后会自动yield,回到主协程，此时状态为TEAM
void Fiber::yield() {
  CondPanic(state_ == TERM || state_ == RUNNING, "state error");
  SetThis(cur_thread_fiber.get());
  if (state_ != TERM) {
    state_ = READY;
  }
  if (isRunInScheduler_) {
    CondPanic(0 == swapcontext(&ctx_, &(Scheduler::GetMainFiber()->ctx_)),
              "isRunInScheduler_ = true,swapcontext error");
  } else {
    // 切换当前协程到主协程，并保存子协程的上下文到主协程ctx_
    CondPanic(0 == swapcontext(&ctx_, &(cur_thread_fiber->ctx_)), "swapcontext failed");
  }
}

// 协程入口函数，调用协程的回调函数cb_，执行完之后将状态设置为TERM，并yield回主协程
void Fiber::MainFunc() {
  Fiber::ptr cur = GetThis();
  CondPanic(cur != nullptr, "cur is nullptr");

  cur->cb_();
  cur->cb_ = nullptr;  //释放 std::function 里可能持有的资源，防止重复执行
  cur->state_ = TERM;
  auto raw_ptr = cur.get(); 
  cur.reset();// 手动使得cur_fiber引用计数减1
  // 协程结束，自动yield,回到主协程
  // 访问原始指针原因：reset后cur（共享指针）已经被释放
  raw_ptr->yield();
}

// 协程重置（复用已经结束的协程，复用其栈空间，换一个新的任务函数，创建新协程）
void Fiber::reset(std::function<void()> cb) {
  CondPanic(stack_ptr, "stack is nullptr");
  CondPanic(state_ == TERM, "state isn't TERM");// 暂时不允许Ready状态下的重置
  cb_ = cb;
  CondPanic(0 == getcontext(&ctx_), "getcontext failed");
  ctx_.uc_link = nullptr;
  ctx_.uc_stack.ss_sp = stack_ptr;
  ctx_.uc_stack.ss_size = stackSize_;
  makecontext(&ctx_, &Fiber::MainFunc, 0);
  state_ = READY;
}

Fiber::~Fiber() {
  --fiber_count;
  if (stack_ptr) {
    // 有栈空间，说明是子协程
    CondPanic(state_ == TERM, "fiber state should be term");
    StackAllocator::Delete(stack_ptr, stackSize_);
    // std::cout << "dealloc stack,id = " << id_ << std::endl;
  } else {
    // 没有栈空间，说明是线程的主协程
    CondPanic(!cb_, "main fiber no callback");
    CondPanic(state_ == RUNNING, "main fiber state should be running");

    Fiber *cur = cur_fiber;
    if (cur == this) {
      SetThis(nullptr);  //cur_fiber==this,说明当前线程的主协程正在被销毁，设置cur_fiber为nullptr
    }
  }
}

}  // namespace monsoon