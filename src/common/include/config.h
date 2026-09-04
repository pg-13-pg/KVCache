//
// Created by swx on 23-12-23.
//

#ifndef CONFIG_H
#define CONFIG_H

// Enable verbose Raft/RPC diagnostics during scenario testing.
const bool Debug = true;

const int debugMul = 1;  // 时间单位：time.Millisecond，不同网络环境rpc速度不同，因此需要乘以一个系数
const int HeartBeatTimeout = 25 * debugMul;  // 心跳时间一般要比选举超时小一个数量级
const int ApplyInterval = 10 * debugMul;     //
const int RaftRpcTimeout = 300 * debugMul;   // ms

// Both the minimum and the random spread exceed one complete peer RPC timeout,
// so a slow response neither triggers nor synchronizes elections.
const int minRandomizedElectionTime = 800 * debugMul;   // ms
const int maxRandomizedElectionTime = 1600 * debugMul;  // ms
static_assert(minRandomizedElectionTime > RaftRpcTimeout);
static_assert(maxRandomizedElectionTime - minRandomizedElectionTime > RaftRpcTimeout);

const int CONSENSUS_TIMEOUT = 500 * debugMul;  // ms

// 协程相关设置

const int FIBER_THREAD_NUM = 1;              // 协程库中线程池大小
const bool FIBER_USE_CALLER_THREAD = false;  // 是否使用caller_thread执行调度任务

#endif  // CONFIG_H
