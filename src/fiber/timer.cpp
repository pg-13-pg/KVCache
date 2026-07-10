#include "timer.hpp"
#include "utils.hpp"

namespace monsoon {
//timer比较器，按定时器的执行时间排序 next_从小到大排序，next_相等时，比较地址，保证set中不会出现重复的Timer
bool Timer::Comparator::operator()(const Timer::ptr &lhs, const Timer::ptr &rhs) const {
  if (!lhs && !rhs) {
    return false;
  }
  if (!lhs) {
    return true;
  }
  if (!rhs) {
    return false;
  }
  if (lhs->next_ < rhs->next_) {
    return true;
  }
  if (rhs->next_ < lhs->next_) {
    return false;
  }
  return lhs.get() < rhs.get();  //lhs和rhs的next_相等时，比较地址，保证set中不会出现重复的Timer
}


Timer::Timer(uint64_t ms, std::function<void()> cb, bool recuring, TimerManager *manager)
    : recurring_(recuring), ms_(ms), cb_(cb), manager_(manager) {
  next_ = GetElapsedMS() + ms_; //当前时间+ms_，得到定时器的到期时间点
}

Timer::Timer(uint64_t next) : next_(next) {}

//取消定时器
bool Timer::cancel() {
  RWMutex::WriteLock lock(manager_->mutex_);  //独占写WriteLock
  if (cb_) {
    cb_ = nullptr;
    auto it = manager_->timers_.find(shared_from_this());  //
    if (it != manager_->timers_.end()) {
       manager_->timers_.erase(it);
       return true;
    }
  }
  return false;
}

//刷新定时器，重新设置定时器触发时间
bool Timer::refresh() {
  RWMutex::WriteLock lock(manager_->mutex_);
  if (!cb_) {
    return false;
  }
  auto it = manager_->timers_.find(shared_from_this());
  if (it == manager_->timers_.end()) {
    return false;
  }
  manager_->timers_.erase(it);
  next_ = GetElapsedMS() + ms_;
  manager_->timers_.insert(shared_from_this());
  return true;
}

// 重置定时器，重新设置定时器触发时间
// from_now = true: 下次出发时间从当前时刻开始计算
// from_now = false: 下次出发时间从上一次起始点开始计算
bool Timer::reset(uint64_t ms, bool from_now) {
  if (ms == ms_ && !from_now) {
    return true;
  }
  RWMutex::WriteLock lock(manager_->mutex_);
  if (!cb_) {
    return true;
  }
  auto it = manager_->timers_.find(shared_from_this());
  if (it == manager_->timers_.end()) {
    return false;
  }
  manager_->timers_.erase(it);
  uint64_t start = 0;
  if (from_now) {
    start = GetElapsedMS();
  } else {
    start = next_ - ms_;
  }
  ms_ = ms;
  next_ = start + ms_;
  manager_->addTimer(shared_from_this(), lock);
  return true;
}

TimerManager::TimerManager() { previouseTime_ = GetElapsedMS(); }

TimerManager::~TimerManager() {}

Timer::ptr TimerManager::addTimer(uint64_t ms, std::function<void()> cb, bool recurring) {
  Timer::ptr timer(new Timer(ms, cb, recurring, this));
  RWMutex::WriteLock lock(mutex_);
  addTimer(timer, lock);//添加到set中，并判断是否需要触发OnTimerInsertedAtFront()，如果是，则触发该函数
  return timer;
}


static void OnTimer(std::weak_ptr<void> weak_cond, std::function<void()> cb) {
  std::shared_ptr<void> tmp = weak_cond.lock();  //将弱引用提升为强引用，如果提升失败，说明对象已经被销毁，则不执行回调函数
  if (tmp) {
    cb();
  }
}

//添加条件定时器，，定时器到时后，只有当weak_cond所指向的对象还存在时，才会执行回调函数cb
Timer::ptr TimerManager::addConditionTimer(uint64_t ms, std::function<void()> cb, std::weak_ptr<void> weak_cond,
                                           bool recurring) {
  return addTimer(ms, std::bind(&OnTimer, weak_cond, cb), recurring);
}

// 返回最早到期定时器距离当前时间的剩余毫秒数；无定时器返回 ~0ull，已到期返回 0。
uint64_t TimerManager::getNextTimer() {
  RWMutex::ReadLock lock(mutex_);  //读锁，保护定时器集合，可以有多个线程同时读取定时器集合，但只有一个线程可以写入定时器集合
  tickled_ = false;
  if (timers_.empty()) {
    return ~0ull; //很大的数，表示没有定时器
  }
  const Timer::ptr &next = *timers_.begin();
  uint64_t now_ms = GetElapsedMS();
  if (now_ms >= next->next_) {  //定时器到期
    return 0;
  } else {
    return next->next_ - now_ms;  //返回距离下一个定时器到期的时间间隔
  }
}

//获取需要执行的定时器的回调函数列表
void TimerManager::listExpiredCb(std::vector<std::function<void()>> &cbs) {
  uint64_t now_ms = GetElapsedMS();
  std::vector<Timer::ptr> expired;
  {
    RWMutex::ReadLock lock(mutex_);
    if (timers_.empty()) {
      return;
    }
  }

  RWMutex::WriteLock lock(mutex_);
  if (timers_.empty()) {
    return;
  }
  bool rollover = false;
  if (detectClockRolllover(now_ms)) {
    rollover = true;
  }
  if (!rollover && ((*timers_.begin())->next_ > now_ms)) { //没有到期的定时器
    return;
  }

  Timer::ptr now_timer(new Timer(now_ms));//创建一个临时定时器对象，next_为当前时间，用于比较定时器集合中哪些定时器已经到期
  auto it = rollover ? timers_.end() : timers_.lower_bound(now_timer);//找到第一个到期的定时器，lower_bound返回第一个不小于now_timer的迭代器
  while (it != timers_.end() && (*it)->next_ == now_ms) { //如果有多个定时器的next_相等，继续向后查找，直到找到第一个不小于now_timer的迭代
    ++it;
  }
  expired.insert(expired.begin(), timers_.begin(), it);
  timers_.erase(timers_.begin(), it);

  cbs.reserve(expired.size());
  for (auto &timer : expired) {
    cbs.push_back(timer->cb_);
    if (timer->recurring_) {
      // 循环计时，重新加入堆中
      timer->next_ = now_ms + timer->ms_;
      timers_.insert(timer);
    } else {
      timer->cb_ = nullptr;//一次性 Timer 自己不再需要保存它，所以清空，用来标记失效和释放资源。
    }
  }
}

//添加到TimerManager的定时器集合中，并判断是否需要触发OnTimerInsertedAtFront()，如果是，则触发该函数
void TimerManager::addTimer(Timer::ptr val, RWMutex::WriteLock &lock) {
  auto it = timers_.insert(val).first;  //std::pair<iterator, bool>
  bool at_front = (it == timers_.begin()) && !tickled_;
  if (at_front) {
    tickled_ = true;
  }
  lock.unlock();
  if (at_front) {
    OnTimerInsertedAtFront();
  }
}

//检测时间是否回退，
bool TimerManager::detectClockRolllover(uint64_t now_ms) {
  bool rollover = false;
  if (now_ms < previouseTime_ && now_ms < (previouseTime_ - 60 * 60 * 1000)) {
    rollover = true;
  }
  previouseTime_ = now_ms;
  return rollover;
}

bool TimerManager::hasTimer() {
  RWMutex::ReadLock lock(mutex_);
  return !timers_.empty();
}

}  // namespace monsoon