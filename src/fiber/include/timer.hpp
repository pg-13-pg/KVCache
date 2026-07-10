#ifndef __MONSOON_TIMER_H__
#  define __MONSOON_TIMER_H__

#  include <memory>
#  include <set>
#  include <vector>
#  include "mutex.hpp"

namespace monsoon {
class TimerManager;

class Timer : public std::enable_shared_from_this<Timer> {
  friend class TimerManager;

 public:
  typedef std::shared_ptr<Timer> ptr;

  bool cancel();
  bool refresh();
  bool reset(uint64_t ms, bool from_now);  //重置定时器的时间间隔

 private:
  Timer(uint64_t ms, std::function<void()> cb, bool recuring, TimerManager *manager);
  Timer(uint64_t next);

  // 是否是循环定时器
  bool recurring_ = false;
  // 执行周期
  uint64_t ms_ = 0;
  // 精确的执行时间 ，定时器的到期时间点，单位毫秒（到期后执行任务）
  uint64_t next_ = 0;
  // 回调函数
  std::function<void()> cb_;
  // 管理器
  TimerManager *manager_ = nullptr;

 private:
  struct Comparator {  //比较器，按定时器的执行时间排序
    bool operator()(const Timer::ptr &lhs, const Timer::ptr &rhs) const;
  };
};


// 定时器管理器，定期扫描定时器集合，执行到期的定时器回调函数
class TimerManager {
  friend class Timer;

 public:
  TimerManager();
  virtual ~TimerManager();
  Timer::ptr addTimer(uint64_t ms, std::function<void()> cb, bool recuring = false);
  Timer::ptr addConditionTimer(uint64_t ms, std::function<void()> cb, std::weak_ptr<void> weak_cond,
                               bool recurring = false);
  // 到最近一个定时器的时间间隔（ms）
  uint64_t getNextTimer();
  // 获取需要执行的定时器的回调函数列表
  void listExpiredCb(std::vector<std::function<void()>> &cbs);
  // 是否有定时器
  bool hasTimer();

 protected:
  // 当有新的定时器插入到定时器首部，执行该函数
  virtual void OnTimerInsertedAtFront() = 0;  //子类实现
  // 将定时器添加到管理器
  void addTimer(Timer::ptr val, RWMutex::WriteLock &lock);

 private:
  // 检测服务器时间是否被调后了
  bool detectClockRolllover(uint64_t now_ms);

  RWMutex mutex_;   // 读写锁，保护定时器集合，可以有多个线程同时读取定时器集合，但只有一个线程可以写入定时器集合
  // 定时器集合
  std::set<Timer::ptr, Timer::Comparator> timers_; //使用set是因为set是有序的，按照定时器的执行时间排序，方便获取最近的定时器
  // 是否触发OnTimerInsertedAtFront
  bool tickled_ = false;
  // 上次扫描时间
  uint64_t previouseTime_ = 0;
};
}  // namespace monsoon

#endif