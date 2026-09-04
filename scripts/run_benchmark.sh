#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
config_path="${1:-$project_root/config/cluster.conf}"
run_dir="$project_root/logs/benchmark-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$run_dir"

echo "benchmark_log=$run_dir/kvbench.log"
echo "config=$config_path"

exec "$project_root/bin/kvbench" \
  --config "$config_path" \
  --threads "${KVBENCH_THREADS:-4}" \
  --duration-sec "${KVBENCH_DURATION_SEC:-60}" \
  --keyspace "${KVBENCH_KEYSPACE:-100000}" \
  --value-size "${KVBENCH_VALUE_SIZE:-256}" \
  --read-ratio "${KVBENCH_READ_RATIO:-50}" \
  --report-interval-sec "${KVBENCH_REPORT_SEC:-5}" \
  --timeout-ms "${KVBENCH_TIMEOUT_MS:-1000}" \
  2>&1 | tee "$run_dir/kvbench.log"
