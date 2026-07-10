# fiber 协程库说明

`src/fiber` 是项目里的自定义协程库，命名空间是 `monsoon`。它提供用户态协程、协程调度器、IO 事件管理、定时器和系统调用 hook。

## 文件结构

核心文件：

- `fiber.hpp / fiber.cpp`：协程本体 `Fiber`。
- `scheduler.hpp / scheduler.cpp`：协程调度器 `Scheduler`。
- `iomanager.hpp / iomanager.cpp`：基于 `epoll` 的 IO 协程调度器 `IOManager`。
- `timer.hpp / timer.cpp`：定时器管理。
- `hook.hpp / hook.cpp`：hook 阻塞系统调用，让阻塞操作在协程中让出执行权。
- `fd_manager.hpp / fd_manager.cpp`：管理 fd 状态，配合 hook 判断 fd 是否 socket、是否非阻塞、超时时间等。
- `thread.hpp / thread.cpp`：线程封装。
- `mutex.hpp`：锁封装。
- `monsoon.h`：统一包含协程库头文件。

示例代码在：

- `example/fiberExample/test_scheduler.cpp`
- `example/fiberExample/test_iomanager.cpp`
- `example/fiberExample/test_hook.cpp`
- `example/fiberExample/server.cpp`

## Fiber

`Fiber` 是协程对象，定义在 `fiber.hpp`。

主要状态：

```cpp
enum State {
  READY,
  RUNNING,
  TERM,
};
```

含义：

- `READY`：协程可运行。
- `RUNNING`：协程正在运行。
- `TERM`：协程执行结束。

主要接口：

```cpp
Fiber(std::function<void()> cb, size_t stackSz = 0, bool run_in_scheduler = true);
void resume();
void yield();
void reset(std::function<void()> cb);
static Fiber::ptr GetThis();
```

底层使用 `ucontext_t` 保存和切换上下文。每个子协程有自己的栈空间和回调函数 `cb_`。

基本执行关系：

```text
创建 Fiber(cb)
  -> resume()
      -> 切换到协程上下文
      -> 执行 cb
      -> cb 执行完成后状态变为 TERM
  -> yield()
      -> 当前协程让出执行权
      -> 回到调度协程或主协程
```

## Scheduler

`Scheduler` 是 N:M 协程调度器，定义在 `scheduler.hpp`。

它管理：

- 线程池 `threadPool_`
- 任务队列 `tasks_`
- 活跃线程数 `activeThreadCnt_`
- idle 线程数 `idleThreadCnt_`
- 调度协程 `rootFiber_`

任务可以是：

```cpp
Fiber::ptr
std::function<void()>
```

添加任务：

```cpp
scheduler(task);
```

内部会把任务包装为 `SchedulerTask` 放入 `tasks_`：

```text
scheduler(...)
  -> schedulerNoLock(...)
      -> tasks_.push_back(task)
  -> tickle()
      -> 唤醒 idle 协程
```

调度线程执行 `Scheduler::run()`，不断从任务队列取任务运行。没有任务时进入 `idle()`。

## IOManager

`IOManager` 定义在 `iomanager.hpp`：

```cpp
class IOManager : public Scheduler, public TimerManager
```

它同时具备：

- `Scheduler` 的协程调度能力
- `TimerManager` 的定时器能力
- `epoll` 的 IO 事件等待能力

主要接口：

```cpp
int addEvent(int fd, Event event, std::function<void()> cb = nullptr);
bool delEvent(int fd, Event event);
bool cancelEvent(int fd, Event event);
bool cancelAll(int fd);
```

事件类型：

```cpp
enum Event {
  NONE = 0x0,
  READ = 0x1,
  WRITE = 0x4,
};
```

基本流程：

```text
addEvent(fd, READ, cb)
  -> 把 fd 注册到 epoll
  -> 保存事件对应的 Fiber 或 callback

epoll_wait 返回事件
  -> IOManager::idle()
  -> 找到 fd 对应 EventContext
  -> scheduler(cb 或 fiber)
  -> 事件处理逻辑重新进入调度队列
```

`IOManager` 里的 `tickleFds_` 用于唤醒阻塞在 `epoll_wait` 的调度线程。

## TimerManager

`TimerManager` 管理定时任务。`IOManager` 继承它后，可以同时等待：

```text
IO 事件
最近一个定时器超时
```

hook 的 `sleep/usleep/nanosleep` 会借助定时器实现：

```text
当前协程调用 sleep
  -> 添加定时器
  -> 当前 Fiber yield
  -> 定时器到期后重新 scheduler 当前 Fiber
```

这样不会阻塞整个线程，只会挂起当前协程。

## hook

`hook.cpp` 会 hook 常见阻塞系统调用：

- `sleep`
- `usleep`
- `nanosleep`
- `socket`
- `connect`
- `accept`
- `read`
- `recv`
- `write`
- `send`
- `close`
- `fcntl`
- `ioctl`
- `getsockopt`
- `setsockopt`

是否启用 hook 由线程局部变量控制：

```cpp
bool is_hook_enable();
void set_hook_enable(bool flag);
```

`Scheduler::run()` 中默认会开启 hook：

```text
调度线程运行
  -> set_hook_enable(true)
```

hook 的意义是：

```text
原本阻塞整个线程的 IO 操作
  -> 变成注册 IO 事件
  -> 当前协程 yield
  -> IO 就绪后恢复当前协程
```

这样一个线程可以承载多个协程任务。

## Raft 中的使用

Raft 节点中持有：

```cpp
std::unique_ptr<monsoon::IOManager> m_ioManager;
```

初始化时创建：

```cpp
m_ioManager = std::make_unique<monsoon::IOManager>(
    FIBER_THREAD_NUM,
    FIBER_USE_CALLER_THREAD
);
```

配置在 `src/common/include/config.h`：

```cpp
const int FIBER_THREAD_NUM = 1;
const bool FIBER_USE_CALLER_THREAD = false;
```

然后把后台循环交给协程调度：

```cpp
m_ioManager->scheduler([this]() -> void {
  this->leaderHearBeatTicker();
});

m_ioManager->scheduler([this]() -> void {
  this->electionTimeOutTicker();
});
```

也就是：

```text
leaderHearBeatTicker
electionTimeOutTicker
```

由 `IOManager` 调度执行。

当前 Raft 中 `applierTicker()` 仍然单独开线程执行，因为它会向 KVServer 的 `applyChan` 推送消息，可能受到状态机处理速度影响。

## 总体调用关系

```text
用户代码 scheduler(cb)
  -> SchedulerTask 入队
  -> Scheduler::run()
      -> 取出任务
      -> 包装/恢复 Fiber
      -> Fiber::resume()
          -> 执行 cb
          -> cb 中可能 sleep/read/write
              -> hook 拦截
              -> 注册 Timer 或 IO 事件
              -> Fiber::yield()
      -> IOManager::idle()
          -> epoll_wait 等待 IO 或 timer
          -> 事件就绪后重新 scheduler 对应 Fiber/cb
```

一句话：

```text
Fiber 负责保存执行上下文。
Scheduler 负责调度协程。
IOManager 负责把协程调度、epoll 和 timer 结合起来。
hook 负责把阻塞系统调用改造成协程挂起和恢复。
```
