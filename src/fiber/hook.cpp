#include "hook.hpp"
#include <dlfcn.h>
#include <cstdarg>
#include <string>
#include "fd_manager.hpp"
#include "fiber.hpp"
#include "iomanager.hpp"
namespace monsoon {
// 当前线程是否启用hook
static thread_local bool t_hook_enable = false;
static int g_tcp_connect_timeout = 5000;


//HOOK_FUN 是要 hook 的 libc 函数清单，用宏 XX 批量对这些函数做同一种操作，比如 dlsym 获取原始函数地址。
#define HOOK_FUN(XX) \
  XX(sleep)          \
  XX(usleep)         \
  XX(nanosleep)      \
  XX(socket)         \
  XX(connect)        \
  XX(accept)         \
  XX(read)           \
  XX(readv)          \
  XX(recv)           \
  XX(recvfrom)       \
  XX(recvmsg)        \
  XX(write)          \
  XX(writev)         \
  XX(send)           \
  XX(sendto)         \
  XX(sendmsg)        \
  XX(close)          \
  XX(fcntl)          \
  XX(ioctl)          \
  XX(getsockopt)     \
  XX(setsockopt)



void hook_init() {
  static bool is_inited = false;   // static 变量只初始化一次，函数结束也不销毁。
  if (is_inited) {
    return;
  }
  // dlsym:Dynamic LinKinf Library.返回指定符号函数的地址
#define XX(name) name##_f = (name##_fun)dlsym(RTLD_NEXT, #name);
  HOOK_FUN(XX);
#undef XX  // 取消宏定义XX，防止后续代码中误用。
}

static uint64_t s_connect_timeout = -1;
struct _HOOKIniter {
  _HOOKIniter() {
    hook_init();
    s_connect_timeout = g_tcp_connect_timeout;
  }
};
// hook_init放在静态对象中，则在main函数执行之前就会获取各个符号地址并保存到全局变量中
static _HOOKIniter s_hook_initer;

bool is_hook_enable() { return t_hook_enable; }

void set_hook_enable(const bool flag) { t_hook_enable = flag; }

////timer_info 是一次 IO 等待的超时状态标记，（是否超时）timer 用它告诉协程：这次等待是因为超时被唤醒的。
struct timer_info {  
  int cnacelled = 0;
};

//系统底层非阻塞，把系统层面的非阻塞 IO，包装成用户层面的同步阻塞式写法。用户层面非阻塞不用修改，直接返回
//do_io() 用来把阻塞式 socket IO 改造成协程式等待：先调用原始 IO，若返回 EAGAIN 表示暂时未就绪，就注册 epoll 事件并让当前协程 yield；
//同时注册超时 timer，若 IO 先就绪就唤醒协程重新尝试 IO，若 timer 先超时就取消事件、唤醒协程并返回 ETIMEDOUT。
template <typename OriginFun, typename... Args>
static ssize_t do_io(int fd, OriginFun fun, const char *hook_fun_name, uint32_t event, int timeout_so, Args &&...args) {
  if (!t_hook_enable) {
    return fun(fd, std::forward<Args>(args)...); //完美转发
  }
  // 为当前文件描述符创建上下文ctx
  FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd);  //单例FdManager管理所有文件描述符的上下文
  if (!ctx) {
    return fun(fd, std::forward<Args>(args)...);
  }
  // 文件已经关闭
  if (ctx->isClose()) {
    errno = EBADF;
    return -1;
  }

  if (!ctx->isSocket() || ctx->getUserNonblock()) {
    return fun(fd, std::forward<Args>(args)...);
  }
  // 获取对应type的fd超时时间
  uint64_t to = ctx->getTimeout(timeout_so);
  std::shared_ptr<timer_info> tinfo(new timer_info);

//label retry: 用于在 EAGAIN 时重新尝试 IO 操作
retry:
  ssize_t n = fun(fd, std::forward<Args>(args)...);
  while (n == -1 && errno == EINTR) {
    // 读取操作被信号中断，继续尝试
    n = fun(fd, std::forward<Args>(args)...);
  }
  if (n == -1 && errno == EAGAIN) {
    // 数据未就绪，添加timer，到期还未就绪，取消事件唤醒协程（放入调度器任务队列）执行响应处理
    IOManager *iom = IOManager::GetThis();
    Timer::ptr timer;
    std::weak_ptr<timer_info> winfo(tinfo);

    if (to != (uint64_t)-1) {
      timer = iom->addConditionTimer(
          to,
          [winfo, fd, iom, event]() {
            auto t = winfo.lock();
            if (!t || t->cnacelled) {  //timer_info不存在或者已经被取消，说明协程已经被唤醒，直接返回
              return;
            }
            t->cnacelled = ETIMEDOUT; //
            iom->cancelEvent(fd, (Event)(event));
          },
          winfo);
    }

    int rt = iom->addEvent(fd, (Event)(event));
    if (rt) {
      std::cout << hook_fun_name << " addEvent(" << fd << ", " << event << ")";
      if (timer) {
        timer->cancel();
      }
      return -1;
    } else {
      Fiber::GetThis()->yield();//当前协程让出CPU，等待IO事件就绪或超时
    //恢复时有两种可能：io事件就绪，或者超时被唤醒
      if (timer) {  //恢复后先取消 timer
        timer->cancel();
      }
      if (tinfo->cnacelled) { //如果已经超时，则返回错误码 ETIMEDOUT，不再尝试IO操作
        errno = tinfo->cnacelled;
        return -1;
      }
      goto retry;  //被io事件唤醒，重新尝试IO操作
    }
  }

  return n;
}

//hook 自定义的同名函数

extern "C" {
#define XX(name) name##_fun name##_f = nullptr; //定义临时宏XX，接受一个参数name，定义一个函数指针变量name_f，类型为name_fun，并初始化为nullptr。
HOOK_FUN(XX);
#undef XX  //取消宏定义XX，防止后续代码中误用。

unsigned int sleep(unsigned int seconds) {
  if (!t_hook_enable) {
    // 不允许hook,则直接使用系统调用
    return sleep_f(seconds);
  }
  // 允许hook,则直接让当前协程退出，seconds秒后再重启（by定时器）
  Fiber::ptr fiber = Fiber::GetThis();
  IOManager *iom = IOManager::GetThis();
  iom->addTimer(seconds * 1000,
                std::bind((void(Scheduler::*)(Fiber::ptr, int thread)) & IOManager::scheduler, iom, fiber, -1));
  Fiber::GetThis()->yield();
  return 0;
}

// usleep 在指定的微妙数内暂停线程运行
int usleep(useconds_t usec) {
  if (!t_hook_enable) {
    // 不允许hook,则直接使用系统调用
    auto ret = usleep_f(usec);
    return 0;
  }
  // 允许hook,则直接让当前协程退出，seconds秒后再重启（by定时器）
  Fiber::ptr fiber = Fiber::GetThis();
  IOManager *iom = IOManager::GetThis();
  iom->addTimer(usec / 1000,
                std::bind((void(Scheduler::*)(Fiber::ptr, int thread)) & IOManager::scheduler, iom, fiber, -1));
  Fiber::GetThis()->yield();
  return 0;
}

// nanosleep 在指定的纳秒数内暂停当前线程的执行
int nanosleep(const struct timespec *req, struct timespec *rem) {
  if (!t_hook_enable) {
    // 不允许hook,则直接使用系统调用
    return nanosleep_f(req, rem);
  }
  // 允许hook,则直接让当前协程退出，seconds秒后再重启（by定时器）
  Fiber::ptr fiber = Fiber::GetThis();
  IOManager *iom = IOManager::GetThis();
  int timeout_s = req->tv_sec * 1000 + req->tv_nsec / 1000 / 1000;
  iom->addTimer(timeout_s,
                std::bind((void(Scheduler::*)(Fiber::ptr, int thread)) & IOManager::scheduler, iom, fiber, -1));
  Fiber::GetThis()->yield();
  return 0;
}

//创建socket，并将其加入FdManager中
int socket(int domain, int type, int protocol) {
  if (!t_hook_enable) {
    return socket_f(domain, type, protocol);
  }
  int fd = socket_f(domain, type, protocol);
  if (fd == -1) {
    return fd;
  }
  // 将fd加入Fdmanager中
  FdMgr::GetInstance()->get(fd, true);
  return fd;
}

//
int connect_with_timeout(int fd, const struct sockaddr *addr, socklen_t addrlen, uint64_t timeout_ms) {
  if (!t_hook_enable) {
    return connect_f(fd, addr, addrlen);
  }
  FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd);
  if (!ctx || ctx->isClose()) {
    errno = EBADF;
    return -1;
  }
  //不是socket，直接调用原始connect
  if (!ctx->isSocket()) {  
    return connect_f(fd, addr, addrlen);
  }
  // fd是否被显式设置为非阻塞模式
  if (ctx->getUserNonblock()) {
    return connect_f(fd, addr, addrlen);
  }

  // 系统调用connect(fd为非阻塞)
  int n = connect_f(fd, addr, addrlen);
  if (n == 0) { //成功
    return 0;
  } else if (n != -1 || errno != EINPROGRESS) {  //失败，且不是EINPROGRESS错误，直接返回
    return n;
  }
  // 返回EINPEOGRESS:正在进行，但是尚未完成
  IOManager *iom = IOManager::GetThis();
  Timer::ptr timer;
  std::shared_ptr<timer_info> tinfo(new timer_info);
  std::weak_ptr<timer_info> winfo(tinfo);

  // 保证超时参数有效
  if (timeout_ms != (uint64_t)-1) {
    // 添加条件定时器
    timer = iom->addConditionTimer(
        timeout_ms,
        [winfo, fd, iom]() {
          auto t = winfo.lock();
          if (!t || t->cnacelled) {
            return;
          }
          //定时时间到达，设置超时标志，触发一次WRITE事件
          t->cnacelled = ETIMEDOUT;
          iom->cancelEvent(fd, WRITE);
        },
        winfo);
  }

  // 添加WRITE事件，并yield,等待WRITE事件触发再往下执行
  int rt = iom->addEvent(fd, WRITE);
  if (rt == 0) {
    Fiber::GetThis()->yield();
    // 等待超时or套接字可写，协程返回
    if (timer) {
      timer->cancel();
    }
    // 超时返回，通过超时标志设置errno并返回-1
    if (tinfo->cnacelled) {
      errno = tinfo->cnacelled;
      return -1;
    }
  } else {
    // addevennt error
    if (timer) {
      timer->cancel();
    }
    std::cout << "connect addEvent(" << fd << ", WRITE) error" << std::endl;
  }
  
  //返回连接结果，获取套接字错误状态
  int error = 0;
  socklen_t len = sizeof(int);
  if (-1 == getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len)) {// 获取套接字的错误状态
    return -1;
  }
  if (!error) {
    return 0;
  } else {
    errno = error;
    return -1;
  }
}

int connect(int sockfd, const struct sockaddr *addr, socklen_t addrlen) {
  return monsoon::connect_with_timeout(sockfd, addr, addrlen, s_connect_timeout);
}

int accept(int s, struct sockaddr *addr, socklen_t *addrlen) {
  int fd = do_io(s, accept_f, "accept", READ, SO_RCVTIMEO, addr, addrlen); //服务端监听read事件
  if (fd >= 0) {
    FdMgr::GetInstance()->get(fd, true);
  }
  return fd;
}
//从 fd 读数据到一块连续缓冲区
ssize_t read(int fd, void *buf, size_t count) { return do_io(fd, read_f, "read", READ, SO_RCVTIMEO, buf, count); }
//从 fd 读数据到多个缓冲区
ssize_t readv(int fd, const struct iovec *iov, int iovcnt) {
  return do_io(fd, readv_f, "readv", READ, SO_RCVTIMEO, iov, iovcnt);
}
//从 fd 读数据到一块连续缓冲区，socket专用读
ssize_t recv(int sockfd, void *buf, size_t len, int flags) {
  return do_io(sockfd, recv_f, "recv", READ, SO_RCVTIMEO, buf, len, flags);
}
//常用于 UDP。除了读数据，还能拿到发送方地址。
ssize_t recvfrom(int sockfd, void *buf, size_t len, int flags, struct sockaddr *src_addr, socklen_t *addrlen) {
  return do_io(sockfd, recvfrom_f, "recvfrom", READ, SO_RCVTIMEO, buf, len, flags, src_addr, addrlen);
}
//最通用、最复杂的 socket 接收接口。
ssize_t recvmsg(int sockfd, struct msghdr *msg, int flags) {
  return do_io(sockfd, recvmsg_f, "recvmsg", READ, SO_RCVTIMEO, msg, flags);
}
// 通用 fd 写，写一块连续 buffer。
ssize_t write(int fd, const void *buf, size_t count) {
  return do_io(fd, write_f, "write", WRITE, SO_SNDTIMEO, buf, count);
}
//通用 fd 写，聚集写，一次写多个 buffer。
ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
  return do_io(fd, writev_f, "writev", WRITE, SO_SNDTIMEO, iov, iovcnt);
}
// socket 写，类似 write，但多 flags 参数。
ssize_t send(int s, const void *msg, size_t len, int flags) {
  return do_io(s, send_f, "send", WRITE, SO_SNDTIMEO, msg, len, flags);
}
//socket 写，带目标地址，常用于 UDP。
ssize_t sendto(int s, const void *msg, size_t len, int flags, const struct sockaddr *to, socklen_t tolen) {
  return do_io(s, sendto_f, "sendto", WRITE, SO_SNDTIMEO, msg, len, flags, to, tolen);
}
//最通用的 socket 发送接口，可以发送多 buffer、目标地址、控制信息。
ssize_t sendmsg(int s, const struct msghdr *msg, int flags) {
  return do_io(s, sendmsg_f, "sendmsg", WRITE, SO_SNDTIMEO, msg, flags);
}

//关闭 fd 前，清理这个 fd 在协程 IO 框架里的状态和等待事件。
int close(int fd) {
  if (!t_hook_enable) {
    return close_f(fd);
  }
  FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd);
  if (ctx) {
    auto iom = IOManager::GetThis();
    if (iom) {
      iom->cancelAll(fd);
    }
    FdMgr::GetInstance()->del(fd);
  }
  return close_f(fd);
}

//hook 版 fcntl 主要是拦截 F_SETFL/F_GETFL，协程库默认socket是非阻塞的，
//记录用户是否主动设置 O_NONBLOCK，do_io() 会根据这个标志决定是否要把系统层面的非阻塞
//同时保证框架内部 socket 仍保持非阻塞；其他命令基本原样转发给原始 fcntl。
int fcntl(int fd, int cmd, ... ) {
  va_list va;  //定义一个可变参数列表，接受可变参数
  va_start(va, cmd);
  switch (cmd) {
    case F_SETFL: {
      int arg = va_arg(va, int); //可变参数列表里取出一个参数，并把它当成 int 类
      va_end(va);
      FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd);
      if (!ctx || ctx->isClose() || !ctx->isSocket()) { //如果fd不存在，或者已经关闭，或者不是socket，则直接调用原始fcntl
        return fcntl_f(fd, cmd, arg);
      }
      ctx->setUserNonblock(arg & O_NONBLOCK);
      if (ctx->getSysNonblock()) {  //强制设置 O_NONBLOCK 标志，保证协程库内部 socket 仍保持非阻塞
        arg |= O_NONBLOCK;
      } else {
        arg &= ~O_NONBLOCK; 
      }
      return fcntl_f(fd, cmd, arg);
    } break;
    case F_GETFL: {
      va_end(va);
      int arg = fcntl_f(fd, cmd);
      FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd);
      if (!ctx || ctx->isClose() || !ctx->isSocket()) {
        return arg;
      }
      if (ctx->getUserNonblock()) {
        return arg | O_NONBLOCK;
      } else {
        return arg & ~O_NONBLOCK;
      }
    } break;
    case F_DUPFD:
    case F_DUPFD_CLOEXEC:
    case F_SETFD:
    case F_SETOWN:
    case F_SETSIG:
    case F_SETLEASE:
    case F_NOTIFY:
#ifdef F_SETPIPE_SZ   //如果定义了 F_SETPIPE_SZ，则处理这个命令
    case F_SETPIPE_SZ:
#endif
    {
      int arg = va_arg(va, int);
      va_end(va);
      return fcntl_f(fd, cmd, arg);
    } break;
    case F_GETFD:
    case F_GETOWN:
    case F_GETSIG:
    case F_GETLEASE:
#ifdef F_GETPIPE_SZ
    case F_GETPIPE_SZ:
#endif
    {
      va_end(va);
      return fcntl_f(fd, cmd);
    } break;
    case F_SETLK:
    case F_SETLKW:
    case F_GETLK: {
      struct flock *arg = va_arg(va, struct flock *);
      va_end(va);
      return fcntl_f(fd, cmd, arg);
    } break;
    case F_GETOWN_EX:
    case F_SETOWN_EX: {
      struct f_owner_exlock *arg = va_arg(va, struct f_owner_exlock *);
      va_end(va);
      return fcntl_f(fd, cmd, arg);
    } break;
    default:
      va_end(va);
      return fcntl_f(fd, cmd);
  }
}

//拦截 FIONBIO，记录用户是否主动设置了非阻塞，后续do_io()会根据这个标志决定是否要把系统层面的非阻塞 IO 包装成协程式等待。
int ioctl(int d, unsigned long int request, ...) {
  va_list va;
  va_start(va, request);
  void *arg = va_arg(va, void *);
  va_end(va);

  if (FIONBIO == request) {
    bool user_nonblock = !!*(int *)arg;
    FdCtx::ptr ctx = FdMgr::GetInstance()->get(d);
    if (!ctx || ctx->isClose() || !ctx->isSocket()) {
      return ioctl_f(d, request, arg);
    }
    ctx->setUserNonblock(user_nonblock);
  }
  return ioctl_f(d, request, arg);
}

int getsockopt(int sockfd, int level, int optname, void *optval, socklen_t *optlen) {
  return getsockopt_f(sockfd, level, optname, optval, optlen);
}

int setsockopt(int sockfd, int level, int optname, const void *optval, socklen_t optlen) {
  if (!t_hook_enable) {
    return setsockopt_f(sockfd, level, optname, optval, optlen);
  }
  if (level == SOL_SOCKET) {
    if (optname == SO_RCVTIMEO || optname == SO_SNDTIMEO) {
      FdCtx::ptr ctx = FdMgr::GetInstance()->get(sockfd);
      if (ctx) {
        const timeval *v = (const timeval *)optval;
        ctx->setTimeout(optname, v->tv_sec * 1000 + v->tv_usec / 1000);
      }
    }
  }
  return setsockopt_f(sockfd, level, optname, optval, optlen);
}
}
}  // namespace monsoon