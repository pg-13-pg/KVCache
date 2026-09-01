#!/usr/bin/env python3

import argparse
import dataclasses
import os
import pathlib
import shutil
import signal
import socket
import subprocess
import sys
import time


@dataclasses.dataclass
class NodeProcess:
    node_id: int
    data_dir: pathlib.Path
    log_path: pathlib.Path
    process: subprocess.Popen[str] | None = None


class RaftCluster:
    def __init__(self, raft_node: str, kvctl: str, artifact_dir: pathlib.Path) -> None:
        self.raft_node = raft_node
        self.kvctl = kvctl
        self.artifact_dir = artifact_dir
        self.config_path = artifact_dir / "cluster.conf"
        self.max_raft_state = 1024 * 1024
        self.nodes = [
            NodeProcess(node_id, artifact_dir / f"node-{node_id}",
                        artifact_dir / f"node-{node_id}.log")
            for node_id in range(3)
        ]
        self.leaders_by_term: dict[int, int] = {}
        for node in self.nodes:
            node.data_dir.mkdir(parents=True, exist_ok=True)

    def _write_reserved_config(self) -> list[socket.socket]:
        reservations: list[socket.socket] = []
        endpoints: list[tuple[str, int]] = []
        for _ in self.nodes:
            reservation = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            reservation.bind(("127.0.0.1", 0))
            reservations.append(reservation)
            endpoints.append(("127.0.0.1", reservation.getsockname()[1]))
        with self.config_path.open("w", encoding="utf-8") as config:
            for node_id, (ip, port) in enumerate(endpoints):
                config.write(f"node{node_id}ip={ip}\n")
                config.write(f"node{node_id}port={port}\n")
        return reservations

    def start_all(self) -> None:
        for attempt in range(2):
            reservations = self._write_reserved_config()
            for reservation in reservations:
                reservation.close()
            for node in self.nodes:
                self.start_node(node.node_id)
            time.sleep(0.3)
            if not self._address_in_use():
                return
            self.cleanup()
            if attempt == 0:
                for node in self.nodes:
                    node.log_path.unlink(missing_ok=True)
                    node.process = None
        raise AssertionError("node bind failed after port allocation retry")

    def _address_in_use(self) -> bool:
        for node in self.nodes:
            if node.log_path.exists() and "Address already in use" in node.log_path.read_text(
                    encoding="utf-8", errors="replace"):
                return True
        return False

    def start_node(self, node_id: int) -> None:
        node = self.nodes[node_id]
        command = [self.raft_node, "--id", str(node_id),
                   "--config", str(self.config_path),
                   "--data-dir", str(node.data_dir),
                   "--max-raft-state", str(self.max_raft_state)]
        log = node.log_path.open("a", encoding="utf-8")
        node.process = subprocess.Popen(
            command, stdout=log, stderr=subprocess.STDOUT,
            text=True, start_new_session=True)
        log.close()

    def stop_node(self, node_id: int,
                  sig: signal.Signals = signal.SIGTERM) -> None:
        process = self.nodes[node_id].process
        if process is None or process.poll() is not None:
            return
        os.killpg(process.pid, sig)
        try:
            process.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=3.0)

    def restart_node(self, node_id: int) -> None:
        self.stop_node(node_id)
        self.start_node(node_id)

    def live_node_ids(self) -> list[int]:
        return [node.node_id for node in self.nodes
                if node.process is not None and node.process.poll() is None]

    def status(self, node_id: int, timeout: float = 1.0) -> dict[str, str]:
        completed = subprocess.run(
            [self.kvctl, "--config", str(self.config_path),
             "--timeout-ms", str(int(timeout * 1000)),
             "status", "--node", str(node_id)],
            text=True, capture_output=True,
            timeout=timeout + 1.0, check=True)
        return dict(field.split("=", 1)
                    for field in completed.stdout.strip().split())

    def record_leader(self, term: int, node_id: int) -> None:
        previous = self.leaders_by_term.setdefault(term, node_id)
        if previous != node_id:
            raise AssertionError(
                f"term {term} observed leaders {previous} and {node_id}")

    def wait_for_leader(self, timeout: float = 10.0) -> tuple[int, int]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            leaders: list[tuple[int, int]] = []
            for node_id in self.live_node_ids():
                try:
                    current = self.status(node_id)
                except (subprocess.SubprocessError, OSError, ValueError, KeyError):
                    continue
                if current["role"] == "LEADER":
                    leaders.append((node_id, int(current["term"])))
            if len(leaders) == 1:
                node_id, term = leaders[0]
                self.record_leader(term, node_id)
                return node_id, term
            time.sleep(0.05)
        raise AssertionError("leader election exceeded 10 seconds")

    def run_kvctl(self, arguments: list[str], timeout: float = 5.0) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [self.kvctl, "--config", str(self.config_path),
             "--timeout-ms", str(int(timeout * 1000)), *arguments],
            text=True, capture_output=True, timeout=timeout + 1.0, check=True)

    def put(self, key: str, value: str, timeout: float = 5.0) -> None:
        self.run_kvctl(["put", key, value], timeout)

    def get(self, key: str, timeout: float = 5.0) -> str:
        return self.run_kvctl(["get", key], timeout).stdout.rstrip("\n")

    def wait_all_applied(self, target: int, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                states = [self.status(node_id) for node_id in self.live_node_ids()]
            except (subprocess.SubprocessError, OSError, ValueError, KeyError):
                time.sleep(0.05)
                continue
            if states and all(int(state["last_applied"]) >= target for state in states):
                return
            time.sleep(0.05)
        raise AssertionError(f"nodes did not apply through {target}")

    def wait_node_caught_up(self, node_id: int, commit_index: int,
                            last_applied: int, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                state = self.status(node_id)
                if (int(state["commit_index"]) >= commit_index and
                        int(state["last_applied"]) >= last_applied):
                    return
            except (subprocess.SubprocessError, OSError, ValueError, KeyError):
                pass
            time.sleep(0.05)
        raise AssertionError(f"node {node_id} did not catch up")

    def cleanup(self) -> None:
        for node in self.nodes:
            self.stop_node(node.node_id)


def scenario_failover(cluster: RaftCluster) -> None:
    cluster.start_all()
    old_leader, old_term = cluster.wait_for_leader()

    for item in range(100):
        cluster.put(f"key-{item:03d}", f"value-{item:03d}")
    for item in range(100):
        actual = cluster.get(f"key-{item:03d}")
        if actual != f"value-{item:03d}":
            raise AssertionError(f"replication mismatch for key-{item:03d}")
    leader_state = cluster.status(old_leader)
    cluster.wait_all_applied(int(leader_state["commit_index"]))

    cluster.stop_node(old_leader, signal.SIGKILL)
    new_leader, new_term = cluster.wait_for_leader()
    if new_term <= old_term:
        raise AssertionError("failover leader did not advance term")
    if new_leader == old_leader:
        raise AssertionError("stopped leader remained leader")

    for item in range(100):
        if cluster.get(f"key-{item:03d}") != f"value-{item:03d}":
            raise AssertionError(f"failover lost key-{item:03d}")
    for item in range(50):
        cluster.put(f"after-failover-{item:03d}", f"value-{item:03d}")
    for item in range(50):
        if cluster.get(f"after-failover-{item:03d}") != f"value-{item:03d}":
            raise AssertionError(f"failover write mismatch {item:03d}")

    target = cluster.status(new_leader)
    cluster.restart_node(old_leader)
    cluster.wait_node_caught_up(old_leader, int(target["commit_index"]),
                                int(target["last_applied"]))


def main() -> int:
    if not sys.platform.startswith("linux"):
        return 77
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True, choices=["failover"])
    parser.add_argument("--raft-node", required=True)
    parser.add_argument("--kvctl", required=True)
    parser.add_argument("--artifact-root", required=True, type=pathlib.Path)
    args = parser.parse_args()

    stamp = time.strftime("%Y%m%d-%H%M%S")
    artifact_dir = (args.artifact_root /
                    f"{args.scenario}-{stamp}-{os.getpid()}").resolve()
    artifact_dir.mkdir(parents=True, exist_ok=False)
    cluster = RaftCluster(args.raft_node, args.kvctl, artifact_dir)
    success = False
    try:
        scenario_failover(cluster)
        success = True
        return 0
    finally:
        cluster.cleanup()
        if success:
            shutil.rmtree(artifact_dir)
        else:
            print(f"retained artifacts: {artifact_dir}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
