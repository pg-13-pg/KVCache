#!/usr/bin/env python3
"""Run a repeatable high-concurrency and Raft fault scenario suite."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path


STATUS_RE = re.compile(
    r"node_id=(?P<node>\d+) term=(?P<term>\d+) role=(?P<role>\w+) "
    r"commit_index=(?P<commit>-?\d+) last_applied=(?P<applied>-?\d+)"
)


class ScenarioSuite:
    def __init__(self, args: argparse.Namespace) -> None:
        self.root = Path(__file__).resolve().parents[1]
        stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        self.run_dir = self.root / "logs" / f"scenario-{stamp}"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir = self.run_dir / "data"
        self.config = self.run_dir / "cluster.conf"
        self.faults = self.run_dir / "raft-faults.policy"
        self.args = args
        self.nodes: dict[int, subprocess.Popen[str]] = {}
        self.node_logs: dict[int, object] = {}
        self.active_benchmark: subprocess.Popen[str] | None = None
        self.stop_requested = False
        self.current_phase = "starting"
        self.status_monitor: threading.Thread | None = None
        self.status_monitor_stop = threading.Event()
        self.events = (self.run_dir / "events.log").open("w", buffering=1)
        self.status_log = (self.run_dir / "status.log").open("w", buffering=1)
        self.summary = self.run_dir / "summary.txt"

    def log(self, kind: str, message: str) -> None:
        line = f"[{dt.datetime.now().strftime('%H:%M:%S')}] [{kind}] {message}"
        print(line, flush=True)
        self.events.write(line + "\n")

    def prepare(self) -> None:
        source = self.root / "config" / "cluster.conf"
        self.config.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
        self.faults.write_text("", encoding="utf-8")
        self.log("run", f"run_dir={self.run_dir}")

    def start_node(self, node_id: int) -> None:
        data = self.data_dir / f"node-{node_id}"
        data.mkdir(parents=True, exist_ok=True)
        command = [
            str(self.root / "bin" / "raftNode"), "--id", str(node_id),
            "--config", str(self.config), "--data-dir", str(data),
            "--max-raft-state", "1048576", "--raft-fault-file", str(self.faults),
        ]
        process = subprocess.Popen(command, cwd=self.root, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, text=True, bufsize=1,
                                   start_new_session=True)
        self.nodes[node_id] = process
        path = self.run_dir / f"node-{node_id}.log"
        self.node_logs[node_id] = path.open("a", buffering=1)
        threading.Thread(target=self.forward_node_log, args=(node_id, process), daemon=True).start()
        self.log("node", f"started node={node_id} pid={process.pid}")

    def forward_node_log(self, node_id: int, process: subprocess.Popen[str]) -> None:
        output = self.node_logs[node_id]
        assert process.stdout is not None
        for line in process.stdout:
            output.write(line)
            print(f"[node{node_id}] {line.rstrip()}", flush=True)
        output.close()

    def status(self, node_id: int) -> dict[str, str] | None:
        command = [str(self.root / "bin" / "kvctl"), "--config", str(self.config),
                   "--timeout-ms", "1000", "status", "--node", str(node_id)]
        try:
            result = subprocess.run(command, cwd=self.root, text=True, capture_output=True,
                                    timeout=3, check=True)
        except (subprocess.SubprocessError, OSError):
            return None
        match = STATUS_RE.search(result.stdout)
        return match.groupdict() if match else None

    def set_phase(self, label: str) -> None:
        self.current_phase = label

    def sample_status(self, label: str, emit: bool = True) -> list[dict[str, str]]:
        statuses = []
        for node_id in range(3):
            state = self.status(node_id)
            if state:
                statuses.append(state)
                self.status_log.write(f"{dt.datetime.now().isoformat()} phase={label} "
                                      f"node_id={state['node']} term={state['term']} role={state['role']} "
                                      f"commit_index={state['commit']} last_applied={state['applied']}\n")
            else:
                self.status_log.write(f"{dt.datetime.now().isoformat()} phase={label} "
                                      f"node_id={node_id} term=- role=UNAVAILABLE "
                                      f"commit_index=- last_applied=-\n")
        if statuses and emit:
            compact = " ".join(f"node{s['node']}:{s['role']}/t{s['term']}/c{s['commit']}/a{s['applied']}"
                               for s in statuses)
            self.log("status", f"phase={label} {compact}")
        return statuses

    def monitor_status(self) -> None:
        while not self.status_monitor_stop.is_set():
            try:
                self.sample_status(self.current_phase, emit=False)
            except (OSError, ValueError):
                pass
            self.status_monitor_stop.wait(1.0)

    def wait_for_leader(self, exclude: int | None = None, timeout: float = 15) -> tuple[int, dict[str, str]]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            states = [self.status(i) for i in range(3)]
            leaders = [(i, s) for i, s in enumerate(states) if s and s["role"] == "LEADER" and i != exclude]
            if leaders:
                return leaders[0]
            time.sleep(0.2)
        raise RuntimeError("leader election timed out")

    def run_benchmark(self, label: str, duration: int) -> None:
        command = [str(self.root / "bin" / "kvbench"), "--config", str(self.config),
                   "--threads", str(self.args.threads), "--duration-sec", str(duration),
                   "--keyspace", str(self.args.keyspace), "--value-size", str(self.args.value_size),
                   "--read-ratio", str(self.args.read_ratio), "--report-interval-sec", "5",
                   "--timeout-ms", str(self.args.timeout_ms)]
        log_path = self.run_dir / "kvbench.log"
        self.log("phase", f"{label} duration_sec={duration} threads={self.args.threads}")
        with log_path.open("a", buffering=1) as log:
            process = subprocess.Popen(command, cwd=self.root, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, text=True, bufsize=1)
            self.active_benchmark = process
            assert process.stdout is not None
            for line in process.stdout:
                log.write(f"[{label}] {line}")
                print(f"[kvbench/{label}] {line.rstrip()}", flush=True)
            try:
                if process.wait() != 0 and not self.stop_requested:
                    raise RuntimeError(f"kvbench phase failed: {label}")
            finally:
                self.active_benchmark = None

    def set_fault(self, rule: str | None) -> None:
        self.faults.write_text((rule + "\n") if rule else "", encoding="utf-8")
        self.log("fault", rule or "clear")

    def stop_node(self, node_id: int, force: bool = True) -> None:
        process = self.nodes.get(node_id)
        if not process or process.poll() is not None:
            return
        self.log("fault", f"leader_stop node={node_id} pid={process.pid} force={force}")
        os.killpg(process.pid, signal.SIGKILL if force else signal.SIGTERM)
        process.wait(timeout=5)

    def restart_node(self, node_id: int) -> None:
        self.log("recovery", f"restart node={node_id}")
        self.start_node(node_id)

    def verify_keys(self) -> tuple[int, int]:
        ok = 0
        total = self.args.verify_keys
        for index in range(total):
            key = f"scenario-final-{index}"
            value = f"value-{index}"
            put = [str(self.root / "bin" / "kvctl"), "--config", str(self.config),
                   "--timeout-ms", str(self.args.timeout_ms), "put", key, value]
            get = [str(self.root / "bin" / "kvctl"), "--config", str(self.config),
                   "--timeout-ms", str(self.args.timeout_ms), "get", key]
            if subprocess.run(put, cwd=self.root).returncode != 0:
                continue
            result = subprocess.run(get, cwd=self.root, text=True, capture_output=True)
            if result.returncode == 0 and result.stdout.strip() == value:
                ok += 1
        return ok, total

    def converge(self, timeout: float = 20) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            states = [self.status(i) for i in range(3)]
            if all(states) and len({s["commit"] for s in states if s}) == 1 and len({s["applied"] for s in states if s}) == 1:
                return True
            time.sleep(0.5)
        return False

    def cleanup(self) -> None:
        for node_id, process in list(self.nodes.items()):
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        for process in self.nodes.values():
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        self.status_monitor_stop.set()
        if self.status_monitor and self.status_monitor.is_alive():
            self.status_monitor.join(timeout=3)
        self.events.close()
        self.status_log.close()

    def run(self) -> int:
        try:
            self.prepare()
            for node_id in range(3):
                self.start_node(node_id)
            self.status_monitor = threading.Thread(target=self.monitor_status,
                                                    name="status-monitor", daemon=True)
            self.status_monitor.start()
            leader, state = self.wait_for_leader()
            self.log("election", f"initial_leader=node{leader} term={state['term']}")
            self.set_phase("high_concurrency_before_failure")
            self.run_benchmark("high_concurrency_before_failure", self.args.duration)
            old_leader = leader
            old_state = self.status(old_leader) or state
            self.set_phase("leader_failure")
            self.stop_node(old_leader)
            self.log("election", f"leader_down=node{old_leader} term={old_state['term']} role={old_state['role']}")
            new_leader, new_state = self.wait_for_leader(exclude=old_leader)
            self.log("election", f"replacement_leader=node{new_leader} term={new_state['term']}")
            self.set_phase("high_concurrency_after_failover")
            self.run_benchmark("high_concurrency_after_failover", self.args.duration)
            self.set_phase("restart")
            self.restart_node(old_leader)
            self.log("recovery", f"restarted_node=node{old_leader}")
            self.set_phase("recovery")
            self.run_benchmark("recovery", self.args.recovery_duration)
            self.set_phase("convergence_after_recovery")
            self.log("verify", f"replicas_converged={self.converge()}")
            leader, state = self.wait_for_leader()
            target = (leader + 1) % 3
            self.set_phase("network_delay")
            self.set_fault(f"{leader} {target} AppendEntries delay {self.args.delay_ms}")
            self.run_benchmark("network_delay", self.args.fault_duration)
            self.set_phase("packet_drop")
            self.set_fault(f"{leader} {target} AppendEntries drop")
            self.run_benchmark("packet_drop", self.args.fault_duration)
            self.set_phase("final_verification")
            self.set_fault(None)
            ok, total = self.verify_keys()
            converged = self.converge()
            final_states = self.sample_status("final")
            self.summary.write_text(
                f"run_dir={self.run_dir}\nverification_keys={ok}/{total}\n"
                f"replicas_converged={'true' if converged else 'false'}\n"
                f"final_nodes={len(final_states)}/3\n", encoding="utf-8")
            self.log("summary", f"verification_keys={ok}/{total} replicas_converged={converged}")
            return 0 if ok == total and converged else 1
        except (RuntimeError, OSError) as error:
            self.log("error", str(error))
            self.summary.write_text(f"run_dir={self.run_dir}\nerror={error}\n", encoding="utf-8")
            return 1
        finally:
            self.cleanup()


def main() -> int:
    parser = argparse.ArgumentParser(description="run high-concurrency Raft business and fault scenarios")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--duration", type=int, default=20, help="seconds for each high-concurrency phase")
    parser.add_argument("--recovery-duration", type=int, default=10)
    parser.add_argument("--fault-duration", type=int, default=10)
    parser.add_argument("--delay-ms", type=int, default=200)
    parser.add_argument("--keyspace", type=int, default=10000)
    parser.add_argument("--value-size", type=int, default=256)
    parser.add_argument("--read-ratio", type=int, default=70)
    parser.add_argument("--timeout-ms", type=int, default=1000)
    parser.add_argument("--verify-keys", type=int, default=20)
    args = parser.parse_args()
    if min(args.threads, args.duration, args.recovery_duration, args.fault_duration, args.keyspace, args.timeout_ms, args.verify_keys) <= 0:
        parser.error("numeric options must be positive")
    if not 0 <= args.read_ratio <= 100 or args.delay_ms < 0 or args.value_size < 0:
        parser.error("invalid ratio, delay, or value size")
    suite = ScenarioSuite(args)
    def request_stop(signum: int, _frame: object) -> None:
        suite.stop_requested = True
        suite.log("signal", f"received={signum}, stopping active workload")
        if suite.active_benchmark and suite.active_benchmark.poll() is None:
            suite.active_benchmark.terminate()
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    return suite.run()


if __name__ == "__main__":
    sys.exit(main())
