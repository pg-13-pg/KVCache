
#ifndef UTIL_H
#define UTIL_H

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/access.hpp>
#include <condition_variable>  // pthread_condition_t
#include <functional>
#include <iostream>
#include <mutex>  // pthread_mutex_t
#include <queue>
#include <random>
#include <sstream>
#include <thread>
#include "config.h"

//和DEFER配合使用，使用RAII的思想，保证在函数结束时执行一些清理工作，比如释放锁，持久化数据等
template <class F>
class DeferClass {
 public:
  DeferClass(F&& f) : m_func(std::forward<F>(f)) {}
  DeferClass(const F& f) : m_func(f) {}
  ~DeferClass() { m_func(); }
  DeferClass(const DeferClass& e) = delete;
  DeferClass& operator=(const DeferClass& e) = delete;
 private:
  F m_func;
};

#define _CONCAT(a, b) a##b //##是预处理器中的连接操作符，用于将两个标识符连接成一个标识符
#define _MAKE_DEFER_(line) DeferClass _CONCAT(defer_placeholder, line) = [&]()
//DeferClass defer_placeholderline= [&](){ persist();}
#undef DEFER
#define DEFER _MAKE_DEFER_(__LINE__) // 这里的__LINE__是预定义的宏，表示当前行号，确保每个defer对象的名字唯一



//打印特定格式的日志，方便调试
void DPrintf(const char* format, ...);

void myAssert(bool condition, std::string message = "Assertion failed!");  //assert
 //字符格式化函数，类似于printf，但返回一个std::string对象，方便日志输出等场景
template <typename... Args>
std::string format(const char* format_str, Args... args) {
    int size_s = std::snprintf(nullptr, 0, format_str, args...) + 1; // "\0" ，算格式化后的字符串需要多少字符
    if (size_s <= 0) { throw std::runtime_error("Error during formatting."); }
    auto size = static_cast<size_t>(size_s);
    std::vector<char> buf(size);
    std::snprintf(buf.data(), size, format_str, args...);
    return std::string(buf.data(), buf.data() + size - 1);  // remove '\0'
}

std::chrono::_V2::system_clock::time_point now();

std::chrono::milliseconds getRandomizedElectionTimeout();
void sleepNMilliseconds(int N);


// 异步写日志的日志队列
// read is blocking!!! LIKE  go chan
template <typename T>
class LockQueue {
 public:
  // 多个worker线程都会写日志queue
  void Push(const T& data) {
    std::lock_guard<std::mutex> lock(m_mutex);  //使用lock_gurad，即RAII的思想保证锁正确释放
    m_queue.push(data);
    m_condvariable.notify_one();
  }

  // 一个线程读日志queue，写日志文件
  T Pop() {
    std::unique_lock<std::mutex> lock(m_mutex);
    while (m_queue.empty()) {
      // 日志队列为空，线程进入wait状态
      m_condvariable.wait(lock);  //这里用unique_lock是因为lock_guard不支持解锁，而unique_lock支持
    }
    T data = m_queue.front();
    m_queue.pop();
    return data;
  }

  bool timeOutPop(int timeout, T* ResData)  // 添加一个超时的Pop，默认为 50 毫秒
  {
    std::unique_lock<std::mutex> lock(m_mutex);

    // 获取当前时间点，并计算出超时时刻
    auto now = std::chrono::system_clock::now();
    auto timeout_time = now + std::chrono::milliseconds(timeout);

    // 在超时之前，不断检查队列是否为空
    while (m_queue.empty()) {
      // 如果已经超时了，就返回一个空对象
      if (m_condvariable.wait_until(lock, timeout_time) == std::cv_status::timeout) {
        return false;
      } else {
        continue;
      }
    }

    T data = m_queue.front();
    m_queue.pop();
    *ResData = data;
    return true;
  }

 private:
  std::queue<T> m_queue;
  std::mutex m_mutex;
  std::condition_variable m_condvariable;
};



// 这个Op是kvserver传递给raft的command（业务层面）
class Op {
 public:
  std::string Operation;  // "Get" "Put" "Append"
  std::string Key;
  std::string Value;
  std::string ClientId;  //客户端号码  防止网络问题导致的重复请求，保证幂等性
  int RequestId;         //客户端号码请求的Request的序列号，为了保证线性一致性（一个客户多次请求）
                         // Duplicate command can't be applied twice , but only for PUT and APPEND

 public:

  //为了协调raftRPC中的command只设置成了string,这个的限制就是正常字符中不能包含|
  //当然后期可以换成更高级的序列化方法，比如protobuf
  //Raft 日志里的 command 字段可能设计成了 string 类型

//序列化
  std::string asString() const {  
    std::stringstream ss;   //字符串流  像cout/cin 读写字符串  //archive:档案
    boost::archive::text_oarchive oa(ss);//Boost Serialization 的文本输出归档器，把对象内容写到 stringstream 里

    oa << *this;  //当前 Op 对象写入 archive
  
    return ss.str();
  }
//反序列化
  bool parseFromString(std::string str) {
    std::stringstream iss(str);
    boost::archive::text_iarchive ia(iss);
    ia >> *this;
    return true;  // todo : 解析失敗如何處理，要看一下boost庫了
  }

 public:
 //重载输出运算符。 std::cout << op << std::endl;
  friend std::ostream& operator<<(std::ostream& os, const Op& obj) {
    os << "[MyClass:Operation{" + obj.Operation + "},Key{" + obj.Key + "},Value{" + obj.Value + "},ClientId{" +
              obj.ClientId + "},RequestId{" + std::to_string(obj.RequestId) + "}]";  // 在这里实现自定义的输出格式
    return os;
  }

 private:
  friend class boost::serialization::access;//调用serialize()
//告诉boost库如何序列化和反序列化Op类的成员变量
  template <class Archive>
  void serialize(Archive& ar, const unsigned int version) { 
    ar& Operation;  //Boost Archive 重载了 operator&  序列化时：ar << Operation;反序列化时：ar >> Operation;
    ar& Key;
    ar& Value;
    ar& ClientId;
    ar& RequestId;
  }
};
// op.asString()
//     |
//     v
// 创建 stringstream ss
//     |
//     v
// 创建 text_oarchive oa(ss)
//     |
//     v
// 执行 oa << op
//     |
//     v
// Boost 发现 op 是 Op 类型
//     |
//     v
// 通过 boost::serialization::access 调用 op.serialize(oa, version)
//     |
//     v
// serialize() 中依次执行：
//     ar & Operation
//     ar & Key
//     ar & Value
//     ar & ClientId
//     ar & RequestId
//     |
//     v
// 字段被写入 ss
//     |
//     v
// ss.str() 返回字符串



///////////////////////////////////////////////kvserver reply err to clerk

const std::string OK = "OK";
const std::string ErrNoKey = "ErrNoKey";
const std::string ErrWrongLeader = "ErrWrongLeader";

////////////////////////////////////获取可用端口

bool isReleasePort(unsigned short usPort);

bool getReleasePort(short& port);

#endif  //  UTIL_H