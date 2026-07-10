#include "iomanager.hpp"

namespace monsoon {

// 获取事件上下文
EventContext &FdContext::getEveContext(Event event) {
  switch (event) {
    case READ:
      return read;
    case WRITE:
      return write;
    default:
      CondPanic(false, "getContext error: unknow event");
  }
  throw std::invalid_argument("getContext invalid event");
}

// 重置事件上下文
void FdContext::resetEveContext(EventContext &ctx) {
  ctx.scheduler = nullptr;
  ctx.fiber.reset();
  ctx.cb = nullptr;
}

// 触发事件（只是将对应的fiber or cb 加入scheduler tasklist）
void FdContext::triggerEvent(Event event) {
  CondPanic(events & event, "event hasn't been registed");
  events = (Event)(events & ~event);  //当前事件已经触发了，从 events 中删除它。
  EventContext &ctx = getEveContext(event);
  if (ctx.cb) {
    ctx.scheduler->scheduler(ctx.cb);  //添加到调度器的任务队列中
  } else {
    ctx.scheduler->scheduler(ctx.fiber);
  }
  resetEveContext(ctx);  //重置事件上下文，释放资源
  return;
}


//初始化 Scheduler，再创建 epoll 和用于唤醒的 pipe，把 pipe 读端注册进 epoll，初始化 fd 事件上下文数组，最后启动调度器
IOManager::IOManager(size_t threads, bool use_caller, const std::string &name) : Scheduler(threads, use_caller, name) {
  epfd_ = epoll_create(5000);
  int ret = pipe(tickleFds_);  //0读 1写
  CondPanic(ret == 0, "pipe error");

  // 注册pipe读句柄的可读事件，用于tickle调度协程
  epoll_event event{};
  memset(&event, 0, sizeof(epoll_event));
  event.events = EPOLLIN | EPOLLET;  //可读，边缘触发
  event.data.fd = tickleFds_[0];
  // 边缘触发，设置非阻塞
  ret = fcntl(tickleFds_[0], F_SETFL, O_NONBLOCK);
  CondPanic(ret == 0, "set fd nonblock error");
  // 注册管道读描述符
  ret = epoll_ctl(epfd_, EPOLL_CTL_ADD, tickleFds_[0], &event);
  CondPanic(ret == 0, "epoll_ctl error");

  contextResize(32);

  // 启动scheduler，开始进行协程调度
  start();
}


IOManager::~IOManager() {
  stop();   //调度器停止，等待所有任务结束
  close(epfd_);  
  close(tickleFds_[0]);
  close(tickleFds_[1]);

  for (size_t i = 0; i < fdContexts_.size(); i++) {
    if (fdContexts_[i]) {
      delete fdContexts_[i];
    }
  }
}

// 添加事件 给指定的fd添加事件，event是READ/WRITE，cb是回调函数，如果cb为空，则将当前协程作为回调任务
int IOManager::addEvent(int fd, Event event, std::function<void()> cb) {
  //拿到fd对应的FdContext，如果没有则创建一个新的FdContext
  FdContext *fd_ctx = nullptr;
  RWMutex::ReadLock lock(mutex_);
  // 找到fd对应的fdCOntext,没有则创建
  if ((int)fdContexts_.size() > fd) {
    fd_ctx = fdContexts_[fd];
    lock.unlock();
  } else {
    lock.unlock();
    RWMutex::WriteLock lock2(mutex_);
    contextResize(fd * 1.5);
    fd_ctx = fdContexts_[fd];
  }

  // 将事件注册到epoll中，同一个fd不允许注册重复事件
  Mutex::Lock ctxLock(fd_ctx->mutex);
  CondPanic(!(fd_ctx->events & event), "addevent error, fd = " + std::to_string(fd));

  int op = fd_ctx->events ? EPOLL_CTL_MOD : EPOLL_CTL_ADD; //添加事件还是修改事件到 epoll
  epoll_event epevent;
  epevent.events = EPOLLET | fd_ctx->events | event; //边缘触发 + 已有事件 + 新增事件
  epevent.data.ptr = fd_ctx;

  int ret = epoll_ctl(epfd_, op, fd, &epevent);
  if (ret) {
    std::cout << "addevent: epoll ctl error" << std::endl;
    return -1;
  }
  ++pendingEventCnt_;// 待执行IO事件数量

  // 赋值fd对应的event事件的EventContext
  fd_ctx->events = (Event)(fd_ctx->events | event); //
  EventContext &event_ctx = fd_ctx->getEveContext(event);
  CondPanic(!event_ctx.scheduler && !event_ctx.fiber && !event_ctx.cb, "event_ctx is nullptr");

  event_ctx.scheduler = Scheduler::GetThis();  //通常是IOManager的调度器 Scheduler -> IOManager
  if (cb) {
    // 设置了回调函数
    event_ctx.cb.swap(cb);
  } else {
    // 未设置回调函数，则将当前协程(当前线程执行的协程)设置为回调任务
    event_ctx.fiber = Fiber::GetThis();  //cur_fiber
    CondPanic(event_ctx.fiber->getState() == Fiber::RUNNING, "state=" + event_ctx.fiber->getState());
  }
  std::cout << "add event success,fd = " << fd << std::endl;
  return 0;
}

// 删除事件  从 epoll 和 FdContext 中删除某个 fd 的指定事件，但不会触发这个事件对应的回调或协程
bool IOManager::delEvent(int fd, Event event) {
  RWMutex::ReadLock lock(mutex_);
  if ((int)fdContexts_.size() <= fd) {
    // 找不到当前事件，返回
    return false;
  }
  FdContext *fd_ctx = fdContexts_[fd];
  lock.unlock();

  Mutex::Lock ctxLock(fd_ctx->mutex);
  if (!(fd_ctx->events & event)) {
    return false;
  }
  // 清理指定事件
  Event new_events = (Event)(fd_ctx->events & ~event);
  int op = new_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
  epoll_event epevent;
  epevent.events = EPOLLET | new_events;
  epevent.data.ptr = fd_ctx;
  // 注册删除事件
  int ret = epoll_ctl(epfd_, op, fd, &epevent);
  if (ret) {
    std::cout << "delevent: epoll_ctl error" << std::endl;
    return false;
  }
  --pendingEventCnt_;
  fd_ctx->events = new_events;
  EventContext &event_ctx = fd_ctx->getEveContext(event);
  fd_ctx->resetEveContext(event_ctx);
  return true;
}

// 取消事件 （取消前会主动触发事件,挂在这个事件上的协程放回调度器）
bool IOManager::cancelEvent(int fd, Event event) {
  RWMutex::ReadLock lock(mutex_);
  if ((int)fdContexts_.size() <= fd) {
    // 找不到当前事件，返回
    return false;
  }
  FdContext *fd_ctx = fdContexts_[fd];
  lock.unlock();

  Mutex::Lock ctxLock(fd_ctx->mutex);
  if (!(fd_ctx->events & event)) {
    return false;
  }
  // 清理指定事件
  Event new_events = (Event)(fd_ctx->events & ~event);
  int op = new_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
  epoll_event epevent;
  epevent.events = EPOLLET | new_events;
  epevent.data.ptr = fd_ctx;
  // 注册删除事件
  int ret = epoll_ctl(epfd_, op, fd, &epevent);
  if (ret) {
    std::cout << "delevent: epoll_ctl error" << std::endl;
    return false;
  }
  // 取消事件时需要主动触发一次：将等待该事件的协程/回调重新加入调度器。
  // 否则事件已从 epoll 中删除，等待者不会再被 IO 就绪事件唤醒，可能一直挂起。
  // 常见场景：read/write/connect 超时或主动取消等待。
  fd_ctx->triggerEvent(event);  
  --pendingEventCnt_;
  return true;
}

// 取消fd所有事件，把这些事件对应的回调/协程主动加入调度器
bool IOManager::cancelAll(int fd) {
  RWMutex::ReadLock lock(mutex_);
  if ((int)fdContexts_.size() <= fd) {
    // 找不到当前事件，返回
    return false;
  }

  FdContext *fd_ctx = fdContexts_[fd];
  lock.unlock();

  Mutex::Lock ctxLock(fd_ctx->mutex);
  if (!fd_ctx->events) {
    return false;
  }

  int op = EPOLL_CTL_DEL;
  epoll_event epevent;
  epevent.events = 0;
  epevent.data.ptr = fd_ctx;
  // 注册删除事件
  int ret = epoll_ctl(epfd_, op, fd, &epevent);
  if (ret) {
    std::cout << "delevent: epoll_ctl error" << std::endl;
    return false;
  }
  // 触发全部已注册事件
  if (fd_ctx->events & READ) {
    fd_ctx->triggerEvent(READ);
    --pendingEventCnt_;
  }
  if (fd_ctx->events & WRITE) {
    fd_ctx->triggerEvent(WRITE);
    --pendingEventCnt_;
  }
  CondPanic(fd_ctx->events == 0, "fd not totally clear");
  return true;
}

IOManager *IOManager::GetThis() { return dynamic_cast<IOManager *>(Scheduler::GetThis()); }

// 通知调度器有任务到来，唤醒idle线程，开始调度任务
void IOManager::tickle() {
  if (!isHasIdleThreads()) { 
    // 此时没有空闲的调度线程
    return;
  }
  // 写pipe管道，使得idle协程能够epoll_wait退出，开始调度任务
  int rt = write(tickleFds_[1], "T", 1);
  CondPanic(rt == 1, "write pipe error");
}

// 调度器无任务则阻塞在idle线程上,
// 当有新事件触发，则退出idle状态，则执行回调函数
// 当有新的调度任务，则退出idle状态，并执行对应任务
void IOManager::idle() {
  // 以此最多检测256个就绪事件
  const uint64_t MAX_EVENTS = 256;
  epoll_event *events = new epoll_event[MAX_EVENTS]();  //events[MAX_EVENTS] ,这里可用vector
  std::shared_ptr<epoll_event> shared_events(events, [](epoll_event *ptr) { delete[] ptr; });//传入自定义删除器，防止内存泄漏

  while (true) {
    //// 获取最近定时器超时时间；当无定时器、无待处理 IO 事件且 Scheduler 已停止时，IOManager 才能退出
    uint64_t next_timeout = 0;
    if (stopping(next_timeout)) {
      std::cout << "name=" << getName() << "idle stopping exit"; //scheduler.name_
      break;
    }

    // 阻塞等待，等待事件发生 或者 定时器超时
    int ret = 0;
    do {
      static const int MAX_TIMEOUT = 5000;

      if (next_timeout != ~0ull) {
        next_timeout = std::min((int)next_timeout, MAX_TIMEOUT);
      } else {
        next_timeout = MAX_TIMEOUT;
      }
      // 阻塞等待事件就绪
      ret = epoll_wait(epfd_, events, MAX_EVENTS, (int)next_timeout);
      if (ret < 0) { //出错
        if (errno == EINTR) {
          // 系统调用被信号中断，重新调用epoll_wait
          continue;
        }
        std::cout << "epoll_wait [" << epfd_ << "] errno,err: " << errno << std::endl;
        break;
      } else {
        break;
      }
    } while (true);

    // 收集所有超时定时器的回调函数，添加到调度器的任务队列中
    std::vector<std::function<void()>> cbs;
    listExpiredCb(cbs);
    if (!cbs.empty()) {
      for (const auto &cb : cbs) {
        scheduler(cb);
      }
      cbs.clear();
    }

    //
    for (int i = 0; i < ret; i++) {
      epoll_event &event = events[i];
      if (event.data.fd == tickleFds_[0]) {
        // pipe管道内数据无意义，只是tickle意义,读完即可  “T”
        uint8_t dummy[256];
        while (read(tickleFds_[0], dummy, sizeof(dummy)) > 0);
        continue;
      }

      //  通过epoll_event的私有指针获取FdContext
      FdContext *fd_ctx = (FdContext *)event.data.ptr;
      Mutex::Lock lock(fd_ctx->mutex);

      // 错误事件 or 挂起事件(对端关闭)
      if (event.events & (EPOLLERR | EPOLLHUP)) {
        std::cout << "error events" << std::endl;
        event.events |= (EPOLLIN | EPOLLOUT) & fd_ctx->events;
      }

      //  获取真实发生的事件类型，可能是 READ/WRITE 或者两者都有
      int real_events = NONE;
      if (event.events & EPOLLIN) {
        real_events |= READ;
      }
      if (event.events & EPOLLOUT) {
        real_events |= WRITE;
      }
      if ((fd_ctx->events & real_events) == NONE) {//如果实际触发的事件和当前 fd 已注册的事件没有交集，则忽略
        continue;
      }

      // 本次触发的事件是一次性的，（需重新添加）需要从 epoll 监听集合中移除。
      // 如果 fd 还有未触发的事件，则用 MOD 保留剩余事件；如果没有剩余事件，则用 DEL 将 fd 从 epoll 中移除。
      int left_events = (fd_ctx->events & ~real_events);
      int op = left_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
      event.events = EPOLLET | left_events;

      int ret2 = epoll_ctl(epfd_, op, fd_ctx->fd, &event);
      if (ret2) {
        std::cout << "epoll_wait [" << epfd_ << "] errno,err: " << errno << std::endl;
        continue;
      }

      // 处理已就绪事件 （加入scheduler tasklist,未调度执行）
      if (real_events & READ) {
        fd_ctx->triggerEvent(READ);
        --pendingEventCnt_;
      }
      if (real_events & WRITE) {
        fd_ctx->triggerEvent(WRITE);
        --pendingEventCnt_;
      }
    }
    // 处理结束，idle协程yield,此时调度协程可以执行run去tasklist中
    // 检测，拿取新任务去调度
    Fiber::ptr cur = Fiber::GetThis();
    auto raw_ptr = cur.get();//获取当前协程的裸指针
    cur.reset();//清掉 shared_ptr
    raw_ptr->yield();  //raw_ptr为Fiber*
  }
}

bool IOManager::stopping() {
  uint64_t timeout = 0;
  return stopping(timeout);
}

//判断是否可以停止，所有待调度的Io事件执行结束后，才允许退出
bool IOManager::stopping(uint64_t &timeout) {
  // 所有待调度的Io事件执行结束后，才允许退出
  timeout = getNextTimer();
  return timeout == ~0ull && pendingEventCnt_ == 0 && Scheduler::stopping();
}

//fdContexts_的大小是动态增长的，fd作为索引，fdContext作为值
void IOManager::contextResize(size_t size) {
  fdContexts_.resize(size);
  for (size_t i = 0; i < fdContexts_.size(); i++) {
    if (!fdContexts_[i]) {
      fdContexts_[i] = new FdContext;
      fdContexts_[i]->fd = i;
    }
  }
}

// 新定时器插到队首，说明最近到期时间被提前；
// 唤醒可能正在 epoll_wait(idle) 的调度线程，使其重新计算 epoll_wait timeout。
void IOManager::OnTimerInsertedAtFront() { tickle(); } 

}  // namespace monsoon
