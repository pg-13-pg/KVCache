#!/usr/bin/env python3

import argparse
import concurrent.futures
import dataclasses
import os
import pathlib
import random
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
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
        self.fault_policy_path = artifact_dir / "raft-faults.policy"
        self.command_log_path = artifact_dir / "commands.log"
        self._command_log_lock = threading.Lock()
        self.max_raft_state = 1024 * 1024
        self.nodes = [
            NodeProcess(node_id, artifact_dir / f"node-{node_id}",
                        artifact_dir / f"node-{node_id}.log")
            for node_id in range(3)
        ]
        self.leaders_by_term: dict[int, int] = {}
        for node in self.nodes:
            node.data_dir.mkdir(parents=True, exist_ok=True)
        self.fault_policy_path.write_text("", encoding="utf-8")

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
                   "--max-raft-state", str(self.max_raft_state),
                   "--raft-fault-file", str(self.fault_policy_path)]
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

    def _run_logged(self, command: list[str], timeout: float,
                    check: bool = True) -> subprocess.CompletedProcess[str]:
        started = time.monotonic()
        completed = subprocess.run(command, text=True, capture_output=True,
                                   timeout=timeout, check=False)
        elapsed = time.monotonic() - started
        with self._command_log_lock:
            with self.command_log_path.open("a", encoding="utf-8") as history:
                history.write(
                    f"elapsed={elapsed:.3f}s rc={completed.returncode} "
                    f"command={shlex.join(command)}\n"
                    f"stdout={completed.stdout!r}\nstderr={completed.stderr!r}\n")
        if check and completed.returncode != 0:
            raise subprocess.CalledProcessError(
                completed.returncode, command, completed.stdout,
                completed.stderr)
        return completed

    def status(self, node_id: int, timeout: float = 1.0) -> dict[str, str]:
        completed = self._run_logged(
            [self.kvctl, "--config", str(self.config_path),
             "--timeout-ms", str(int(timeout * 1000)),
             "status", "--node", str(node_id)], timeout + 1.0)
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

    def wait_for_higher_term_leader(self, isolated_node: int, old_term: int,
                                    timeout: float = 10.0) -> tuple[int, int]:
        """Find a replacement leader while the isolated node may still claim leadership."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            leaders: list[tuple[int, int]] = []
            for node_id in self.live_node_ids():
                if node_id == isolated_node:
                    continue
                try:
                    current = self.status(node_id)
                except (subprocess.SubprocessError, OSError, ValueError, KeyError):
                    continue
                if current["role"] == "LEADER" and int(current["term"]) > old_term:
                    leaders.append((node_id, int(current["term"])))
            if len(leaders) == 1:
                node_id, term = leaders[0]
                self.record_leader(term, node_id)
                return node_id, term
            time.sleep(0.05)
        raise AssertionError("isolated leader did not yield to a higher-term majority leader")

    def set_fault_policy(self, rules: list[str]) -> None:
        """Atomically replace the policy so RPC readers see complete files only."""
        contents = "\n".join(rules)
        if contents:
            contents += "\n"
        with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=self.artifact_dir,
                prefix="raft-faults-", suffix=".tmp", delete=False) as replacement:
            replacement.write(contents)
            replacement.flush()
            os.fsync(replacement.fileno())
            replacement_path = pathlib.Path(replacement.name)
        os.replace(replacement_path, self.fault_policy_path)

    def clear_fault_policy(self) -> None:
        self.set_fault_policy([])

    def run_kvctl(self, arguments: list[str], timeout: float = 5.0) -> subprocess.CompletedProcess[str]:
        try:
            return self._run_logged(
                [self.kvctl, "--config", str(self.config_path),
                 "--timeout-ms", str(int(timeout * 1000)), *arguments],
                timeout + 1.0)
        except subprocess.CalledProcessError:
            for node_id in self.live_node_ids():
                try:
                    self.status(node_id)
                except (subprocess.SubprocessError, OSError, ValueError, KeyError):
                    pass
            raise

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

    def assert_idle_wal_stable(self) -> None:
        time.sleep(0.2)
        before = [(node.data_dir / "raft.wal").stat().st_size
                  for node in self.nodes]
        time.sleep(0.5)
        after = [(node.data_dir / "raft.wal").stat().st_size
                 for node in self.nodes]
        if after != before:
            raise AssertionError(
                f"idle heartbeats persisted Raft state: before={before} after={after}")

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

    def wait_node_snapshot(self, node_id: int, snapshot_index: int,
                           last_applied: int, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                state = self.status(node_id)
                if (int(state["snapshot_index"]) >= snapshot_index and
                        int(state["last_applied"]) >= last_applied):
                    return
            except (subprocess.SubprocessError, OSError, ValueError, KeyError):
                pass
            time.sleep(0.05)
        raise AssertionError(f"node {node_id} did not install snapshot")

    def stop_all(self, sig: signal.Signals = signal.SIGTERM) -> None:
        for node in self.nodes:
            self.stop_node(node.node_id, sig)

    def restart_all(self) -> None:
        for node in self.nodes:
            self.start_node(node.node_id)

    def cleanup(self) -> None:
        self.stop_all()


def write_snapshot_workload(cluster: RaftCluster, prefix: str,
                            start: int = 0, limit: int = 200,
                            snapshot_after: int = 0) -> list[tuple[str, str]]:
    written: list[tuple[str, str]] = []
    for item in range(start, limit):
        key = f"{prefix}-{item:03d}"
        value = f"value-{item:03d}-" + chr(ord("a") + item % 26) * 128
        cluster.put(key, value)
        written.append((key, value))
        try:
            states = [cluster.status(node_id)
                      for node_id in cluster.live_node_ids()]
        except (subprocess.SubprocessError, OSError, ValueError, KeyError):
            continue
        if (states and all(int(state["snapshot_index"]) > snapshot_after
                          for state in states)):
            return written
    raise AssertionError(f"snapshot did not advance beyond {snapshot_after}")


def corrupt_final_wal_payload(wal_path: pathlib.Path) -> None:
    data = bytearray(wal_path.read_bytes())
    header_size = 28
    offset = 0
    final_payload: tuple[int, int] | None = None
    while offset < len(data):
        if len(data) - offset < header_size or data[offset:offset + 4] != b"KVRW":
            raise AssertionError("WAL does not end at a complete record")
        raft_length = int.from_bytes(data[offset + 16:offset + 20], "big")
        snapshot_length = int.from_bytes(data[offset + 20:offset + 24], "big")
        payload_length = raft_length + snapshot_length
        record_end = offset + header_size + payload_length
        if record_end > len(data):
            raise AssertionError("WAL final record is incomplete")
        final_payload = (offset + header_size, payload_length)
        offset = record_end
    if final_payload is None or final_payload[1] == 0:
        raise AssertionError("WAL final record has no payload to corrupt")
    data[final_payload[0]] ^= 0x01
    wal_path.write_bytes(data)


def scenario_failover(cluster: RaftCluster) -> None:
    cluster.start_all()
    old_leader, old_term = cluster.wait_for_leader()

    large_value = "large-value-" + "x" * 4096
    cluster.put("large-key", large_value)
    if cluster.get("large-key") != large_value:
        raise AssertionError("large value did not survive framed RPC transport")

    for item in range(100):
        cluster.put(f"key-{item:03d}", f"value-{item:03d}")
    for item in range(100):
        actual = cluster.get(f"key-{item:03d}")
        if actual != f"value-{item:03d}":
            raise AssertionError(f"replication mismatch for key-{item:03d}")
    leader_state = cluster.status(old_leader)
    cluster.wait_all_applied(int(leader_state["commit_index"]))
    cluster.assert_idle_wal_stable()

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


def scenario_restart(cluster: RaftCluster) -> None:
    cluster.start_all()
    leader, _ = cluster.wait_for_leader()
    for item in range(100):
        cluster.put(f"restart-{item:03d}", f"value-{item:03d}")
    committed = int(cluster.status(leader)["commit_index"])
    cluster.wait_all_applied(committed)

    cluster.stop_all(signal.SIGKILL)
    cluster.restart_all()
    cluster.wait_for_leader()
    cluster.wait_all_applied(committed)
    for item in range(100):
        actual = cluster.get(f"restart-{item:03d}")
        if actual != f"value-{item:03d}":
            raise AssertionError(f"restart lost key restart-{item:03d}")


def scenario_snapshot_restart(cluster: RaftCluster) -> None:
    cluster.max_raft_state = 4096
    cluster.start_all()
    cluster.wait_for_leader()
    written = write_snapshot_workload(cluster, "snapshot-restart")
    committed = max(int(cluster.status(node_id)["commit_index"])
                    for node_id in cluster.live_node_ids())
    cluster.wait_all_applied(committed)

    cluster.stop_all(signal.SIGKILL)
    cluster.restart_all()
    cluster.wait_for_leader()
    for index in sorted({0, len(written) // 2, len(written) - 1}):
        key, expected = written[index]
        if cluster.get(key) != expected:
            raise AssertionError(f"snapshot restart lost key {key}")


def scenario_snapshot_catchup(cluster: RaftCluster) -> None:
    cluster.max_raft_state = 4096
    cluster.start_all()
    leader, _ = cluster.wait_for_leader()
    initial = write_snapshot_workload(cluster, "snapshot-catchup", limit=40)
    leader_state = cluster.status(leader)
    cluster.wait_all_applied(int(leader_state["commit_index"]))

    follower = next(node_id for node_id in cluster.live_node_ids()
                    if node_id != leader)
    follower_applied = int(cluster.status(follower)["last_applied"])
    cluster.stop_node(follower, signal.SIGKILL)
    additional = write_snapshot_workload(
        cluster, "snapshot-catchup", start=len(initial),
        snapshot_after=follower_applied)

    leader, _ = cluster.wait_for_leader()
    target = cluster.status(leader)
    cluster.restart_node(follower)
    cluster.wait_node_snapshot(follower, int(target["snapshot_index"]),
                               int(target["last_applied"]))

    cluster.stop_node(leader, signal.SIGKILL)
    cluster.wait_for_leader()
    for key, expected in initial + additional:
        if cluster.get(key) != expected:
            raise AssertionError(f"snapshot catchup lost key {key}")


def scenario_wal_tail(cluster: RaftCluster) -> None:
    cluster.start_all()
    leader, _ = cluster.wait_for_leader()
    for item in range(20):
        cluster.put(f"wal-tail-{item:03d}", f"value-{item:03d}")
    target = cluster.status(leader)
    cluster.wait_all_applied(int(target["commit_index"]))

    follower = next(node_id for node_id in cluster.live_node_ids()
                    if node_id != leader)
    cluster.stop_node(follower, signal.SIGKILL)
    wal_path = cluster.nodes[follower].data_dir / "raft.wal"
    with wal_path.open("ab") as wal:
        wal.write(b"KVRW\x00\x01")
    cluster.restart_node(follower)
    cluster.wait_node_caught_up(follower, int(target["commit_index"]),
                                int(target["last_applied"]))

    cluster.stop_node(follower, signal.SIGKILL)
    corrupt_dir = cluster.artifact_dir / "corrupt-node"
    corrupt_dir.mkdir()
    corrupt_wal = corrupt_dir / "raft.wal"
    shutil.copy2(wal_path, corrupt_wal)
    corrupt_final_wal_payload(corrupt_wal)

    command = [cluster.raft_node, "--id", str(follower),
               "--config", str(cluster.config_path),
               "--data-dir", str(corrupt_dir),
               "--max-raft-state", str(cluster.max_raft_state),
               "--raft-fault-file", str(cluster.fault_policy_path)]
    process = subprocess.Popen(command, text=True, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=5.0)
    except subprocess.TimeoutExpired as error:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=3.0)
        raise AssertionError("checksum-corrupt node did not exit") from error
    if process.returncode == 0:
        raise AssertionError("checksum-corrupt node exited successfully")
    if "checksum" not in stderr.lower():
        raise AssertionError(
            f"checksum failure missing from stderr; stdout={stdout!r} stderr={stderr!r}")


def scenario_transport_faults(cluster: RaftCluster) -> None:
    cluster.start_all()
    old_leader, old_term = cluster.wait_for_leader()
    cluster.put("fault-baseline", "before-partition")

    # Isolate the original leader in both directions. The remaining majority
    # must elect a strictly newer term while the old process is still alive.
    isolation_rules = []
    for peer in range(len(cluster.nodes)):
        if peer == old_leader:
            continue
        isolation_rules.extend((f"{old_leader} {peer} any drop",
                                f"{peer} {old_leader} any drop"))
    isolation_rules.append("any any RequestVote duplicate")
    cluster.set_fault_policy(isolation_rules)
    new_leader, new_term = cluster.wait_for_higher_term_leader(old_leader, old_term)
    if new_term <= old_term:
        raise AssertionError("partition replacement term did not advance")
    cluster.put("fault-majority", "committed-during-partition")

    cluster.clear_fault_policy()
    target = cluster.status(new_leader)
    cluster.wait_node_caught_up(old_leader, int(target["commit_index"]),
                                int(target["last_applied"]))
    if cluster.get("fault-majority") != "committed-during-partition":
        raise AssertionError("healed leader did not converge on committed value")

    # Delay one follower only; the other follower still provides quorum.
    followers = [node.node_id for node in cluster.nodes if node.node_id != new_leader]
    delayed = followers[0]
    cluster.set_fault_policy([f"{new_leader} {delayed} AppendEntries delay 75"])
    for item in range(6):
        cluster.put(f"fault-delay-{item}", f"delay-value-{item}")
    healthy = followers[1]
    committed = int(cluster.status(new_leader)["commit_index"])
    cluster.wait_node_caught_up(new_leader, committed, committed)
    cluster.wait_node_caught_up(healthy, committed, committed)
    cluster.clear_fault_policy()
    cluster.wait_node_caught_up(delayed, committed, committed)

    # Drop one follower's AppendEntries while quorum progress continues.
    dropped = followers[1]
    cluster.set_fault_policy([f"{new_leader} {dropped} AppendEntries drop"])
    for item in range(6):
        cluster.put(f"fault-drop-{item}", f"drop-value-{item}")
    committed = int(cluster.status(new_leader)["commit_index"])
    cluster.wait_node_caught_up(new_leader, committed, committed)
    cluster.clear_fault_policy()
    cluster.wait_node_caught_up(dropped, committed, committed)

    # Duplicate delivery exercises receiver idempotence for both RPC methods.
    duplicate_target = followers[0]
    cluster.set_fault_policy([
        f"{new_leader} {duplicate_target} AppendEntries duplicate",
        "any any RequestVote duplicate",
    ])
    for item in range(4):
        cluster.put(f"fault-duplicate-{item}", f"duplicate-value-{item}")
    cluster.clear_fault_policy()
    final_commit = int(cluster.status(new_leader)["commit_index"])
    cluster.wait_all_applied(final_commit)
    for item in range(4):
        expected = f"duplicate-value-{item}"
        if cluster.get(f"fault-duplicate-{item}") != expected:
            raise AssertionError(f"duplicate delivery lost key fault-duplicate-{item}")

    # Deterministic bounded concurrent clients write unique keys, then every
    # acknowledged value is read back after the cluster has converged.
    generator = random.Random(20260902)
    jobs = []
    for client_id in range(4):
        client_jobs = []
        for sequence in range(6):
            key = f"concurrent-{client_id}-{sequence}"
            value = f"client-{client_id}-" + "".join(
                generator.choice("abcdef0123456789") for _ in range(48))
            client_jobs.append((key, value))
        jobs.append(client_jobs)

    def write_client(client_jobs: list[tuple[str, str]]) -> list[tuple[str, str]]:
        for key, value in client_jobs:
            cluster.put(key, value, timeout=8.0)
        return client_jobs

    acknowledged: list[tuple[str, str]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for result in executor.map(write_client, jobs):
            acknowledged.extend(result)
    cluster.clear_fault_policy()
    final_commit = max(int(cluster.status(node_id)["commit_index"])
                       for node_id in cluster.live_node_ids())
    cluster.wait_all_applied(final_commit, timeout=20.0)
    for key, expected in acknowledged:
        if cluster.get(key, timeout=8.0) != expected:
            raise AssertionError(f"concurrent workload lost acknowledged key {key}")


def main() -> int:
    if not sys.platform.startswith("linux"):
        return 77
    parser = argparse.ArgumentParser()
    scenarios = {
        "failover": scenario_failover,
        "restart": scenario_restart,
        "snapshot_restart": scenario_snapshot_restart,
        "snapshot_catchup": scenario_snapshot_catchup,
        "wal_tail": scenario_wal_tail,
        "transport_faults": scenario_transport_faults,
    }
    parser.add_argument("--scenario", required=True, choices=sorted(scenarios))
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
        scenarios[args.scenario](cluster)
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
