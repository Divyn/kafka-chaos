"""More consumers or more threads? Time 6 consumer setups on the same 50,000 events.

Run on the Docker host after `docker compose up -d` in the folder above:
    python3 scaling.py fill            # once: 50,000 order events into a 12 partition topic
    python3 scaling.py race [passes]   # every setup, each pass in a new consumer group

The handler stands in for a slow job such as a database write: it writes one small
JSON file per event and calls fsync, so it waits on the disk for each event.

Each setup starts its consumer processes at once and the clock runs until the
50,000th event has been handled (startup and the group join are inside the time).
Offsets are not committed: every pass reads the topic from the start in a new group,
so every setup handles the same 50,000 events. When a partition moves to another
consumer during the group join, some events are read twice; the handler skips a
file that already exists, the clock waits for 50,000 different events, and the
repeats are printed as "read twice".
"""

import json
import multiprocessing as mp
import os
import shutil
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = "localhost:19092,localhost:19093,localhost:19094"
TOPIC = "orders-scaling"
PARTITIONS = 12
EVENTS = 50_000
OUT = "/root/scaling-out"

# name: (consumer processes, worker threads per consumer; 0 means the handler runs in the poll loop)
SETUPS = {
    "1 consumer": (1, 0),
    "3 consumers": (3, 0),
    "6 consumers": (6, 0),
    "1 consumer, 4 threads": (1, 4),
    "1 consumer, 16 threads": (1, 16),
    "3 consumers, 4 threads each": (3, 4),
}


def fill() -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    topic = NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=3, config={"min.insync.replicas": "2"})
    for f in admin.create_topics([topic]).values():
        try:
            f.result()
        except Exception as e:
            print("topic:", e)
    p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "linger.ms": 20})
    for i in range(EVENTS):
        event = {"order_id": i, "symbol": "RELIANCE", "side": "BUY" if i % 2 else "SELL", "qty": 1 + i % 50, "price": 1400 + i % 100}
        while True:
            try:
                p.produce(TOPIC, key=str(i), value=json.dumps(event))
                break
            except BufferError:
                p.poll(0.1)
        p.poll(0)
    p.flush()
    print(f"filled {EVENTS} events into {TOPIC}, {PARTITIONS} partitions")


def handle(msg) -> bool:
    """Write the event to its own file and fsync it. Returns False if this event was
    already handled (a partition moved to another consumer and was read again)."""
    path = f"{OUT}/{msg.partition()}-{msg.offset()}.json"
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        return False
    os.write(fd, msg.value())
    os.fsync(fd)
    os.close(fd)
    return True


def consumer_process(group: str, threads: int, done, dupes, stop) -> None:
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group, "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.subscribe([TOPIC])
    lock = threading.Lock()
    pool = ThreadPoolExecutor(threads) if threads else None
    room = threading.Semaphore(threads * 4) if threads else None

    def count(new: bool) -> None:
        with done.get_lock():
            if new:
                done.value += 1
            else:
                dupes.value += 1

    def work(msg):
        try:
            count(handle(msg))
        finally:
            room.release()

    while not stop.is_set():
        msg = c.poll(0.2)
        if msg is None or msg.error():
            continue
        if pool:
            room.acquire()
            pool.submit(work, msg)
        else:
            count(handle(msg))
    if pool:
        pool.shutdown(wait=True)
    c.close()


def run_setup(name: str, procs: int, threads: int, group: str) -> float:
    shutil.rmtree(OUT, ignore_errors=True)
    os.makedirs(OUT)
    done = mp.Value("i", 0)
    dupes = mp.Value("i", 0, lock=False)
    stop = mp.Event()
    start = time.perf_counter()
    ps = [mp.Process(target=consumer_process, args=(group, threads, done, dupes, stop)) for _ in range(procs)]
    for p in ps:
        p.start()
    while done.value < EVENTS:
        time.sleep(0.01)
        if time.perf_counter() - start > 900:
            print(f"{name}: timed out at {done.value}")
            break
    elapsed = time.perf_counter() - start
    stop.set()
    for p in ps:
        p.join(30)
        if p.is_alive():
            p.terminate()
    files = len(os.listdir(OUT))
    print(f"{name}: {elapsed:.2f} s, {done.value} handled, {files} files, {dupes.value} read twice", flush=True)
    return elapsed


def race(passes: int) -> None:
    print(f"# {os.cpu_count()} vCPU, {os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') // 2**20} MB RAM")
    print(f"# start {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}, {EVENTS} events, {PARTITIONS} partitions")
    times = {name: [] for name in SETUPS}
    for p in range(1, passes + 1):
        for name, (procs, threads) in SETUPS.items():
            group = f"scaling-{p}-{procs}x{threads}-{int(time.time())}"
            times[name].append(run_setup(f"pass {p}: {name}", procs, threads, group))
    print(f"# end {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
    print("setup,median_s,events_per_s,pass_s")
    for name, ts in sorted(times.items(), key=lambda kv: -statistics.median(kv[1])):
        m = statistics.median(ts)
        print(f"{name},{m:.2f},{EVENTS / m:.0f},{';'.join(f'{t:.2f}' for t in ts)}")


if __name__ == "__main__":
    if sys.argv[1] == "fill":
        fill()
    else:
        race(int(sys.argv[2]) if len(sys.argv) > 2 else 3)
