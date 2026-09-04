#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
config_path="${1:-$project_root/config/cluster.conf}"
key="${2:-name}"
value="${3:-kvraft}"
timeout_ms="${KVCTL_TIMEOUT_MS:-3000}"
kvctl="$project_root/bin/kvctl"

if [[ ! -x "$kvctl" ]]; then
  echo "missing executable: $kvctl" >&2
  echo "build the project first" >&2
  exit 1
fi
if [[ ! -f "$config_path" ]]; then
  echo "missing cluster config: $config_path" >&2
  exit 1
fi

run_kvctl() {
  "$kvctl" --config "$config_path" --timeout-ms "$timeout_ms" "$@"
}

echo "cluster status"
for id in 0 1 2; do
  if ! run_kvctl status --node "$id"; then
    echo "node$id is unavailable" >&2
  fi
done

echo "put: $key=$value"
run_kvctl put "$key" "$value"

echo "get: $key"
run_kvctl get "$key"
