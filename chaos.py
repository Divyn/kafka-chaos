"""Kill a broker and record what the producer and the consumer see.

Run on the Docker host as root: python3 chaos.py [rounds] [gap_seconds] [victims]

With no victims it kills the broker that leads partition 0. Each round prints
how long acknowledgements stopped for, how long the partitions took to get a
new leader, and how many events the producer could not get acknowledged.
"""

import os
import signal
import subprocess
import sys
import time

from confluent_kafka.admin import AdminClient

BOOTSTRAP = "localhost:19092,localhost:19093,localhost:19094"
TOPIC = "orders"
WINDOW = 25.0


def _docker(*args: str) -> str:
    """Run one docker command and return its output."""
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, check=True
    ).stdout


def _partitions() -> dict[int, dict]:
    """Read the leader and the number of in-sync copies of every partition."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP, "log_level": 0,
                         "metadata.max.age.ms": 500})
    metadata = admin.list_topics(TOPIC, timeout=10)
    if TOPIC not in metadata.topics:
        return {}
    return {
        part.id: {"leader": part.leader, "isr": len(part.isrs)}
        for part in metadata.topics[TOPIC].partitions.values()
    }


def _kill(container: str) -> None:
    """Kill the broker's process, the way a real crash would."""
    pid = int(_docker("inspect", "-f", "{{.State.Pid}}", container).strip())
    os.kill(pid, signal.SIGKILL)


def _acks(start: float, end: float) -> list[float]:
    """The times the broker acknowledged an event, inside the window."""
    try:
        with open("producer.log") as log:
            return sorted(float(line.split(",")[0]) for line in log
                          if ",ok," in line and start <= float(line.split(",")[0]) <= end)
    except FileNotFoundError:
        return []


def _errors(since: float) -> int:
    """Events the broker never accepted, from the producer's log."""
    try:
        with open("producer.log") as log:
            return sum(1 for line in log
                       if ",error," in line and float(line.split(",")[0]) >= since)
    except FileNotFoundError:
        return 0


def _round(victims: list[str]) -> None:
    """Kill the brokers, then measure the stall, the election and the restart."""
    broker = int(victims[0].split("-")[1])
    led = {p for p, row in _partitions().items() if row["leader"] == broker}
    start = time.time()
    for name in victims:
        _kill(name)

    new_leader = 0.0
    while time.time() - start < WINDOW:
        if not new_leader and led:
            now = _partitions()
            if all(now.get(p, {}).get("leader") != broker for p in led):
                new_leader = time.time() - start
        time.sleep(0.2)

    end = start + WINDOW
    acks = _acks(start - 2.0, end)
    points = [start - 2.0] + acks + [end]
    stall = max(b - a for a, b in zip(points, points[1:]))

    print(f"{stall:8.2f} s  {new_leader:>9.1f} s  {_errors(start):>6}  {len(led):>5}")


def main() -> None:
    rounds = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    gap = int(sys.argv[2]) if len(sys.argv) > 2 else 12
    victims = sys.argv[3].split(",") if len(sys.argv) > 3 else None

    print("round  killed        stall  new leader  errors  partitions")
    for number in range(1, rounds + 1):
        target = victims
        if target is None:
            leader = _partitions()[0]["leader"]
            target = [f"kafka-{leader}"]
        print(f"{number:<6} {','.join(target):<13}", end="", flush=True)
        _round(target)
        time.sleep(gap)


if __name__ == "__main__":
    main()
