# fiber 阅读指南

这份指南按推荐顺序说明如何阅读 `src/fiber` 协程库。建议先理解“怎么用”，再看“怎么调度”，最后看 IO 和 hook。

## 1. 先看示例

先从示例代码开始：

```text
example/fiberExample/test_scheduler.cpp
example/fiberExample/test_iomanager.cpp
example/fiberExample/test_hook.cpp
```

重点关注：

- `Scheduler::scheduler(...)` 怎么提交任务。
- `IOManager::scheduler(...)` 怎么提交协程任务。
- `IOManager::addEvent(...)` 怎么注册 IO 事件。
- hook 后 `sleep/usleep/read/write` 这类阻塞调用有什么变化。

先建立直觉：

```text
用户提交任务
  -> Scheduler/IOManager 调度
  -> Fiber 执行
  -> 遇到 IO 或 sleep 时 yield
  -> 事件就绪或定时器到期后恢复执行
```

## 2. 阅读 Fiber

文件：

```text
src/fiber/include/fiber.hpp
src/fiber/fiber.cpp
```

重点看：

- `Fiber` 的状态：`READY`、`RUNNING`、`TERM`
- `Fiber::GetThis()`
- `Fiber::resume()`
- `Fiber::yield()`
- `Fiber::MainFunc()`

目标：

```text
理解一个协程如何创建、切入、让出、结束。
```

可以带着这些问题看：

- 每个协程的栈在哪里？
- `ucontext_t ctx_` 保存了什么？
- `resume()` 是从哪里切到哪里？
- `yield()` 又切回哪里？
- 回调函数 `cb_` 执行完后状态如何变成 `TERM`？

## 3. 阅读 Scheduler

文件：

```text
src/fiber/include/scheduler.hpp
src/fiber/scheduler.cpp
```

重点看：

- `SchedulerTask`
- `Scheduler::scheduler(...)`
- `Scheduler::start()`
- `Scheduler::run()`
- `Scheduler::idle()`
- `Scheduler::tickle()`

目标：

```text
理解调度器如何管理任务队列，并在线程中运行 Fiber。
```

核心模型：

```text
任务进入 tasks_
  -> 调度线程运行 Scheduler::run()
  -> 从 tasks_ 取出 Fiber 或 callback
  -> resume 执行
  -> 没任务时进入 idle
```

这一层不用关心 epoll，先把普通协程调度看懂。

## 4. 阅读 TimerManager

文件：

```text
src/fiber/include/timer.hpp
src/fiber/timer.cpp
```

重点看：

- `Timer`
- `TimerManager::addTimer(...)`
- `TimerManager::getNextTimer()`
- `TimerManager::listExpiredCb(...)`

目标：

```text
理解定时器如何保存超时任务，以及如何取出已经超时的回调。
```

后面看 `IOManager` 和 `hook sleep/usleep` 时会用到这一层。

## 5. 阅读 IOManager

文件：

```text
src/fiber/include/iomanager.hpp
src/fiber/iomanager.cpp
```

重点看：

- `IOManager : public Scheduler, public TimerManager`
- `IOManager::addEvent(...)`
- `IOManager::idle()`
- `FdContext`
- `EventContext`
- `tickleFds_`

目标：

```text
理解 IOManager 如何把 epoll、timer 和协程调度结合起来。
```

核心模型：

```text
addEvent(fd, READ/WRITE)
  -> 注册到 epoll
  -> 保存对应 Fiber 或 callback

idle()
  -> epoll_wait 等待 IO 或 timer
  -> 事件就绪后 triggerEvent
  -> 把 Fiber/callback 重新放回 Scheduler
```

这一层是协程库的关键：它让一个线程可以等待多个 IO，并在事件就绪后恢复对应协程。

## 6. 最后阅读 hook

文件：

```text
src/fiber/include/hook.hpp
src/fiber/hook.cpp
src/fiber/include/fd_manager.hpp
src/fiber/fd_manager.cpp
```

建议先看简单函数：

```text
sleep
usleep
nanosleep
```

再看 IO 函数：

```text
read
recv
write
send
connect
accept
```

目标：

```text
理解阻塞系统调用如何变成“挂起当前协程，而不是阻塞整个线程”。
```

核心模型：

```text
协程中调用 read/recv
  -> hook 拦截
  -> 如果暂时不可读，注册 READ 事件
  -> 当前 Fiber yield
  -> epoll 检测到可读
  -> IOManager 重新调度该 Fiber
  -> Fiber 恢复执行
```

## 7. 回到 Raft 中看使用点

文件：

```text
src/raftCore/raft.cpp
src/raftCore/include/raft.h
src/common/include/config.h
```

重点看 `Raft::init()`：

```cpp
m_ioManager = std::make_unique<monsoon::IOManager>(
    FIBER_THREAD_NUM,
    FIBER_USE_CALLER_THREAD
);

m_ioManager->scheduler([this]() {
  this->leaderHearBeatTicker();
});

m_ioManager->scheduler([this]() {
  this->electionTimeOutTicker();
});
```

配置：

```cpp
const int FIBER_THREAD_NUM = 1;
const bool FIBER_USE_CALLER_THREAD = false;
```

理解：

```text
Raft 把心跳定时器和选举超时定时器交给 IOManager 调度。
```

## 推荐阅读路线

最短路线：

```text
test_scheduler.cpp
  -> fiber.hpp / fiber.cpp
  -> scheduler.hpp / scheduler.cpp
  -> test_iomanager.cpp
  -> iomanager.hpp / iomanager.cpp
  -> raft.cpp 的 Raft::init()
```

完整路线：

```text
test_scheduler.cpp
  -> fiber.hpp / fiber.cpp
  -> scheduler.hpp / scheduler.cpp
  -> timer.hpp / timer.cpp
  -> test_iomanager.cpp
  -> iomanager.hpp / iomanager.cpp
  -> test_hook.cpp
  -> hook.hpp / hook.cpp
  -> fd_manager.hpp / fd_manager.cpp
  -> raft.cpp 的 Raft::init()
```

## 阅读时先抓住一句话

```text
Fiber 保存执行上下文。
Scheduler 决定哪个 Fiber 运行。
TimerManager 处理超时。
IOManager 用 epoll 等 IO 事件，并把就绪事件重新交给 Scheduler。
hook 把阻塞系统调用改造成 Fiber yield 和 resume。
```
