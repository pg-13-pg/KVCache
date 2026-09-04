#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
timestamp="$(date '+%Y%m%d-%H%M%S')"
run_dir="$project_root/logs/cluster-$timestamp"
config_source="$project_root/config/cluster.conf"

if [[ ! -x "$project_root/bin/raftNode" ]]; then
  echo "missing executable: $project_root/bin/raftNode" >&2
  echo "build the project first" >&2
  exit 1
fi
if [[ ! -x "$project_root/bin/kvctl" ]]; then
  echo "missing executable: $project_root/bin/kvctl" >&2
  echo "build the project first" >&2
  exit 1
fi
if [[ ! -f "$config_source" ]]; then
  echo "missing cluster config: $config_source" >&2
  exit 1
fi

mkdir -p "$run_dir" "$run_dir/data"
cp "$config_source" "$run_dir/cluster.conf"
: > "$run_dir/raft-faults.policy"

node_pids=()
cleanup() {
  trap - INT TERM EXIT
  for pid in "${node_pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  for pid in "${node_pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
  echo "cluster stopped"
}
trap cleanup INT TERM EXIT

for id in 0 1 2; do
  mkdir -p "$run_dir/data/node-$id"
  "$project_root/bin/raftNode" \
    --id "$id" \
    --config "$run_dir/cluster.conf" \
    --data-dir "$run_dir/data/node-$id" \
    --max-raft-state 1048576 \
    --raft-fault-file "$run_dir/raft-faults.policy" \
    > "$run_dir/node-$id.log" 2>&1 &
  echo $! > "$run_dir/node-$id.pid"
  node_pids+=("$!")
done

printf 'cluster started\nlog directory: %s\nconfig: %s\n' "$run_dir" "$run_dir/cluster.conf"
for id in 0 1 2; do
  printf 'node%d pid: %s\n' "$id" "$(<"$run_dir/node-$id.pid")"
done
printf '\nwatch logs:\n  tail -f %s/node-*.log\n' "$run_dir"

wait
