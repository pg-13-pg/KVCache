#include <atomic>
#include <iostream>
#include <vector>
#include "monsoon.h"

std::atomic<int> completedThreads{0};

void func1()
{
    std::cout << "name: " << monsoon::Thread::GetThis()->GetName() << ",id: " << monsoon::GetThreadId() << std::endl;
    ++completedThreads;
}

void func2()
{
    std::cout << "name: " << monsoon::Thread::GetName() << ",id: " << monsoon::GetThreadId() << std::endl;
}

int main(int argc, char **argv)
{
    std::vector<monsoon::Thread::ptr> tpool;
    for (int i = 0; i < 5; i++)
    {
        monsoon::Thread::ptr t(new monsoon::Thread(&func1, "name_" + std::to_string(i)));
        tpool.push_back(t);
    }

    for (int i = 0; i < 5; i++)
    {
        tpool[i]->join();
    }

    if (completedThreads != 5) {
        std::cerr << "expected 5 worker threads, completed " << completedThreads << std::endl;
        return 1;
    }
    std::cout << "-----thread_test end-----" << std::endl;
    return 0;
}
//创建 5 个线程，每个线程执行 func1，打印线程名字和线程 ID，然后主线程等待这 5 个线程全部结束
