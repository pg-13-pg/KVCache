#include "clerk.h"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include <mutex>
#include "cluster_config.h"
#include "raftServerRpcUtil.h"

namespace {

using Clock = std::chrono::steady_clock;
std::atomic<bool> g_stop{false};

struct Options {
  std::string config;
  int threads = 1;
  int durationSec = 60;
  int keyspace = 10000;
  int valueSize = 128;
  int readRatio = 70;
  int reportSec = 5;
  int timeoutMs = 1000;
};

struct Metrics {
  std::atomic<std::uint64_t> operations{0};
  std::atomic<std::uint64_t> successes{0};
  std::atomic<std::uint64_t> failures{0};
  std::atomic<std::uint64_t> timeouts{0};
  std::atomic<std::uint64_t> latencyUs{0};
  std::atomic<std::uint64_t> buckets[7]{};
  std::mutex writesMutex;
  std::vector<std::string> successfulWrites;
};

void Stop(int) { g_stop.store(true, std::memory_order_relaxed); }

int ParseInt(std::string_view text, const char* option) {
  int value = 0;
  const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
  if (text.empty() || result.ec != std::errc{} || result.ptr != text.data() + text.size()) {
    throw std::runtime_error(std::string("invalid value for ") + option);
  }
  return value;
}

void Usage(std::ostream& out) {
  out << "Usage: kvbench --config PATH [options]\n"
         "  --threads N                 worker threads (default 1)\n"
         "  --duration-sec N            run duration (default 60)\n"
         "  --keyspace N                random key count (default 10000)\n"
         "  --value-size N              value bytes (default 128)\n"
         "  --read-ratio N              reads percent, 0..100 (default 70)\n"
         "  --report-interval-sec N     report interval (default 5)\n"
         "  --timeout-ms N              per-RPC timeout (default 1000)\n"
         "  --help                      show this help\n";
}

Options Parse(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg = argv[i];
    if (arg == "--help") {
      Usage(std::cout);
      std::exit(0);
    }
    if (i + 1 >= argc) throw std::runtime_error("missing value for " + std::string(arg));
    const auto value = std::string_view(argv[++i]);
    if (arg == "--config") options.config = value;
    else if (arg == "--threads") options.threads = ParseInt(value, "--threads");
    else if (arg == "--duration-sec") options.durationSec = ParseInt(value, "--duration-sec");
    else if (arg == "--keyspace") options.keyspace = ParseInt(value, "--keyspace");
    else if (arg == "--value-size") options.valueSize = ParseInt(value, "--value-size");
    else if (arg == "--read-ratio") options.readRatio = ParseInt(value, "--read-ratio");
    else if (arg == "--report-interval-sec") options.reportSec = ParseInt(value, "--report-interval-sec");
    else if (arg == "--timeout-ms") options.timeoutMs = ParseInt(value, "--timeout-ms");
    else throw std::runtime_error("unknown option: " + std::string(arg));
  }
  if (options.config.empty() || options.threads <= 0 || options.durationSec <= 0 ||
      options.keyspace <= 0 || options.valueSize < 0 || options.readRatio < 0 ||
      options.readRatio > 100 || options.reportSec <= 0 || options.timeoutMs <= 0) {
    throw std::runtime_error("invalid benchmark options");
  }
  return options;
}

void RecordLatency(Metrics* metrics, std::uint64_t us) {
  metrics->latencyUs.fetch_add(us, std::memory_order_relaxed);
  constexpr std::uint64_t limits[] = {100, 500, 1000, 5000, 10000, 50000};
  std::size_t bucket = 6;
  for (std::size_t i = 0; i < std::size(limits); ++i) {
    if (us <= limits[i]) { bucket = i; break; }
  }
  metrics->buckets[bucket].fetch_add(1, std::memory_order_relaxed);
}

void Worker(const Options& options, Metrics* metrics, int workerId) {
  Clerk clerk;
  clerk.Init(options.config, std::chrono::milliseconds(options.timeoutMs));
  std::mt19937_64 random(static_cast<std::uint64_t>(Clock::now().time_since_epoch().count()) ^
                         (static_cast<std::uint64_t>(workerId) << 32));
  std::uniform_int_distribution<int> keyDist(0, options.keyspace - 1);
  std::uniform_int_distribution<int> percent(1, 100);
  const std::string value(static_cast<std::size_t>(options.valueSize), 'x');
  while (!g_stop.load(std::memory_order_relaxed)) {
    const std::string key = "bench-" + std::to_string(keyDist(random));
    const bool read = percent(random) <= options.readRatio;
    const auto start = Clock::now();
    ClerkStatus status;
    if (read) {
      std::string result;
      status = clerk.GetUntil(key, &result, start + std::chrono::milliseconds(options.timeoutMs));
      if (status == ClerkStatus::NotFound) status = ClerkStatus::Ok;
    } else {
      status = clerk.PutUntil(key, value, start + std::chrono::milliseconds(options.timeoutMs));
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start).count();
    metrics->operations.fetch_add(1, std::memory_order_relaxed);
    RecordLatency(metrics, static_cast<std::uint64_t>(elapsed));
    if (status == ClerkStatus::Ok) {
      metrics->successes.fetch_add(1, std::memory_order_relaxed);
      if (!read) {
        std::lock_guard<std::mutex> lock(metrics->writesMutex);
        metrics->successfulWrites.push_back(key);
      }
    }
    else {
      metrics->failures.fetch_add(1, std::memory_order_relaxed);
      if (status == ClerkStatus::TimedOut) metrics->timeouts.fetch_add(1, std::memory_order_relaxed);
    }
  }
}

void Verify(const Options& options, Metrics* metrics) {
  Clerk clerk;
  clerk.Init(options.config, std::chrono::milliseconds(options.timeoutMs));
  std::vector<std::string> keys;
  {
    std::lock_guard<std::mutex> lock(metrics->writesMutex);
    keys = metrics->successfulWrites;
  }
  std::sort(keys.begin(), keys.end());
  keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
  std::uint64_t readbackOk = 0;
  for (const auto& key : keys) {
    std::string value;
    if (clerk.GetUntil(key, &value, Clock::now() + std::chrono::milliseconds(options.timeoutMs)) == ClerkStatus::Ok) {
      ++readbackOk;
    }
  }
  const auto endpoints = LoadClusterConfig(options.config);
  int referenceCommit = -1;
  int referenceApplied = -1;
  bool replicasConverged = true;
  for (std::size_t i = 0; i < endpoints.size(); ++i) {
    raftServerRpcUtil rpc(endpoints[i].ip, endpoints[i].port,
                          std::chrono::milliseconds(options.timeoutMs));
    raftKVRpcProctoc::StatusReply status;
    if (!rpc.GetStatus(&status)) {
      replicasConverged = false;
      continue;
    }
    if (referenceCommit < 0) {
      referenceCommit = status.commit_index();
      referenceApplied = status.last_applied();
    } else if (status.commit_index() != referenceCommit || status.last_applied() != referenceApplied) {
      replicasConverged = false;
    }
  }
  std::cout << "verification writes=" << keys.size()
            << " readback_ok=" << readbackOk
            << " readback_failed=" << (keys.size() - readbackOk)
            << " replicas_converged=" << (replicasConverged ? "true" : "false") << std::endl;
}

std::uint64_t Percentile(const Metrics& metrics, std::uint64_t total, double percentile) {
  const std::uint64_t target = static_cast<std::uint64_t>(std::ceil(total * percentile));
  std::uint64_t cumulative = 0;
  constexpr std::uint64_t values[] = {100, 500, 1000, 5000, 10000, 50000, 50001};
  for (std::size_t i = 0; i < std::size(values); ++i) {
    cumulative += metrics.buckets[i].load(std::memory_order_relaxed);
    if (cumulative >= target) return values[i];
  }
  return values[std::size(values) - 1];
}

void Report(const Metrics& metrics, std::uint64_t previous, Clock::time_point previousTime) {
  const auto now = Clock::now();
  const auto operations = metrics.operations.load(std::memory_order_relaxed);
  const auto elapsed = std::chrono::duration<double>(now - previousTime).count();
  const auto intervalOps = operations - previous;
  const auto total = std::max<std::uint64_t>(1, operations);
  const auto average = metrics.latencyUs.load(std::memory_order_relaxed) / total;
  std::cout << std::fixed << std::setprecision(1)
            << "ops=" << operations << " interval_ops=" << intervalOps
            << " throughput=" << (intervalOps / elapsed) << "/s"
            << " success=" << metrics.successes.load() << " failure=" << metrics.failures.load()
            << " timeout=" << metrics.timeouts.load() << " avg_us=" << average
            << " p95_us<=" << Percentile(metrics, total, .95)
            << " p99_us<=" << Percentile(metrics, total, .99) << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    std::cout.setf(std::ios::unitbuf);
    const Options options = Parse(argc, argv);
    std::signal(SIGINT, Stop);
    std::signal(SIGTERM, Stop);
    Metrics metrics;
    std::vector<std::thread> workers;
    workers.reserve(static_cast<std::size_t>(options.threads));
    for (int i = 0; i < options.threads; ++i) workers.emplace_back(Worker, std::cref(options), &metrics, i);
    const auto deadline = Clock::now() + std::chrono::seconds(options.durationSec);
    auto nextReport = Clock::now() + std::chrono::seconds(options.reportSec);
    std::uint64_t previous = 0;
    auto previousTime = Clock::now();
    while (!g_stop.load(std::memory_order_relaxed) && Clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      if (Clock::now() >= nextReport) {
        Report(metrics, previous, previousTime);
        previous = metrics.operations.load();
        previousTime = Clock::now();
        nextReport += std::chrono::seconds(options.reportSec);
      }
    }
    g_stop.store(true, std::memory_order_relaxed);
    for (auto& worker : workers) worker.join();
    Report(metrics, previous, previousTime);
    Verify(options, &metrics);
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "kvbench: " << error.what() << '\n';
    Usage(std::cerr);
    return 64;
  }
}
