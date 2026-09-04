#!/usr/bin/env python3
"""Run a concurrent-write, leader-failover, recovery, and consistency scenario."""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import pathlib
import signal
import socket
import subprocess
import sys
import time


ROOT = pathlib.Path(__file__).resolve().parents[1]


def free_ports(count: int) -> list[int]:
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


class Scenario:
    def __init__(self, writes: int, workers: int, timeout_ms: int) -> None:
        stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        self.run_dir = ROOT / "logs" / f"business-{stamp}"
        self.data_dir = self.run_dir / "data"
        self.config = self.run_dir / "cluster.conf"
        self.faults = self.run_dir / "raft-faults.policy"
        self.raft_node = ROOT / "bin" / "raftNode"
        self.kvctl = ROOT / "bin" / "kvctl"
        self.writes = writes
        self.workers = workers
        self.timeout_ms = timeout_ms
        self.processes: dict[int, subprocess.Popen[bytes]] = {}
        self.ports = free_ports(3)
        self.keys: list[str] = []

    def prepare(self) -> None:
        if not self.raft_node.exists() or not self.kvctl.exists():
            raise RuntimeError("bin/raftNode and bin/kvctl must be built first")
        self.run_dir.mkdir(parents=True)
        self.data_dir.mkdir()
        self.faults.touch()
        with self.config.open("w", encoding="utf-8") as output:
            for node_id, port in enumerate(self.ports):
                output.write(f"node{node_id}ip=127.0.0.1\n")
                output.write(f"node{node_id}port={port}\n")

    def start_node(self, node_id: int) -> None:
        node_data = self.data_dir / f"node-{node_id}"
        node_data.mkdir(parents=True, exist_ok=True)
        log = (self.run_dir / f"node-{node_id}.log").open("ab")
        process = subprocess.Popen(
            [str(self.raft_node), "--id", str(node_id), "--config", str(self.config),
             "--data-dir", str(node_data), "--max-raft-state", "1048576",
             "--raft-fault-file", str(self.faults)],
            stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        log.close()
        self.processes[node_id] = process

    def start_all(self) -> None:
        for node_id in range(3):
            self.start_node(node_id)
        time.sleep(0.5)

    def stop_node(self, node_id: int, force: bool = False) -> None:
        process = self.processes.get(node_id)
        if process is None or process.poll() is not None:
            return
        signal_name = signal.SIGKILL if force else signal.SIGTERM
        process.send_signal(signal_name)
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)

    def cleanup(self) -> None:
        for node_id in list(self.processes):
            self.stop_node(node_id)

    def ctl(self, *args: str, timeout: float = 5.0) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(self.kvctl), "--config", str(self.config),
             "--timeout-ms", str(self.timeout_ms), *args],
            capture_output=True, text=True, timeout=timeout, check=False)

    def status(self, node_id: int) -> dict[str, str] | None:
        result = self.ctl("status", "--node", str(node_id), timeout=2)
        if result.returncode != 0:
            return None
        return dict(field.split("=", 1) for field in result.stdout.split())

    def leader(self, timeout: float = 15.0, exclude: int | None = None) -> tuple[int, int]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            leaders = []
            for node_id in range(3):
                if node_id == exclude:
                    continue
                current = self.status(node_id)
                if current and current.get("role") == "LEADER":
                    leaders.append((node_id, int(current["term"])))
            if len(leaders) == 1:
                return leaders[0]
            time.sleep(0.1)
        raise RuntimeError("leader election timed out")

    def put(self, key: str, value: str) -> bool:
        return self.ctl("put", key, value, timeout=8).returncode == 0

    def get(self, node_id: int, key: str) -> str | None:
        # kvctl discovers the leader for get; node-specific consistency is checked via status.
        result = self.ctl("get", key, timeout=8)
        return result.stdout.strip() if result.returncode == 0 else None

    def concurrent_writes(self, start: int, count: int) -> tuple[int, int]:
        keys = [f"business:item:{index}" for index in range(start, start + count)]
        self.keys.extend(keys)
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as pool:
            results = list(pool.map(lambda key: self.put(key, f"value-{key.rsplit(':', 1)[1]}"), keys))
        return sum(results), len(results)

    def wait_for_convergence(self, timeout: float = 20.0) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            states = [self.status(node_id) for node_id in range(3)]
            if all(states):
                commit_indexes = {state["commit_index"] for state in states if state}
                applied_indexes = {state["last_applied"] for state in states if state}
                if len(commit_indexes) == 1 and len(applied_indexes) == 1:
                    return next(iter(applied_indexes))
            time.sleep(0.2)
        raise RuntimeError("replica indexes did not converge before timeout")

    def validate(self) -> tuple[int, list[str]]:
        inconsistencies = []
        key_errors = 0
        for key in self.keys:
            expected = f"value-{key.rsplit(':', 1)[1]}"
            value = self.get(0, key)
            if value != expected:
                key_errors += 1
                inconsistencies.append(f"{key}: expected={expected} actual={value}")
        try:
            applied_index = self.wait_for_convergence()
            print(f"replicas_converged=commit/last_applied:{applied_index}")
        except RuntimeError as error:
            inconsistencies.append(str(error))
        return len(self.keys) - key_errors, inconsistencies


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--writes", type=int, default=100, help="writes per phase")
    parser.add_argument("--workers", type=int, default=16, help="concurrent clients")
    parser.add_argument("--timeout-ms", type=int, default=3000)
    args = parser.parse_args()
    if args.writes <= 0 or args.workers <= 0:
        parser.error("--writes and --workers must be positive")

    scenario = Scenario(args.writes, args.workers, args.timeout_ms)
    try:
        scenario.prepare()
        scenario.start_all()
        leader, term = scenario.leader()
        print(f"run_dir={scenario.run_dir}")
        print(f"initial_leader=node{leader} term={term}")

        first_ok, first_total = scenario.concurrent_writes(0, args.writes)
        print(f"phase1_writes={first_ok}/{first_total}")

        scenario.stop_node(leader, force=True)
        replacement, replacement_term = scenario.leader(exclude=leader)
        print(f"leader_stopped=node{leader}")
        print(f"replacement_leader=node{replacement} term={replacement_term}")

        second_ok, second_total = scenario.concurrent_writes(args.writes, args.writes)
        print(f"phase2_writes={second_ok}/{second_total}")

        scenario.start_node(leader)
        recovered, recovered_term = scenario.leader()
        print(f"restarted_node=node{leader} current_leader=node{recovered} term={recovered_term}")
        time.sleep(1)

        valid, missing = scenario.validate()
        print(f"consistency={valid}/{len(scenario.keys)}")
        if missing:
            print("inconsistencies:")
            print("\n".join(missing[:20]))
            return 1
        print("business scenario passed")
        return 0
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"business scenario failed: {error}", file=sys.stderr)
        print(f"run_dir={scenario.run_dir}", file=sys.stderr)
        return 1
    finally:
        scenario.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
