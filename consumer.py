"""Read the order events and record what arrives, and when.

Run on the Docker host: python3 consumer.py [seconds]

consumer.log: one line per event received, with the order number, partition and offset.
lag.log: one line every second with the number of events still waiting to be read.
"""

import sys
import time

from confluent_kafka import Consumer

BOOTSTRAP = "localhost:19092,localhost:19093,localhost:19094"
TOPIC = "orders"
GROUP = "chaos-reader"


def _lag(consumer: Consumer) -> int:
    """Count the events in the topic that this consumer has not read yet."""
    waiting = 0
    for tp in consumer.assignment():
        try:
            _, high = consumer.get_watermark_offsets(tp, timeout=1.0)
            position = consumer.position(tp)
        except Exception:
            continue
        if position is not None:
            waiting += max(0, high - position)
    return waiting


def main() -> None:
    seconds = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "auto.commit.interval.ms": 1000,
    })
    consumer.subscribe([TOPIC])
    deadline = time.time() + seconds if seconds else None

    with open("consumer.log", "w") as records, open("lag.log", "w") as lags:
        last_lag = 0.0
        try:
            while deadline is None or time.time() < deadline:
                message = consumer.poll(0.05)
                if message is not None and not message.error():
                    records.write(
                        f"{time.time():.3f},{message.key().decode()},"
                        f"{message.partition()},{message.offset()}\n"
                    )
                    records.flush()
                now = time.time()
                if now - last_lag >= 1.0:
                    last_lag = now
                    lags.write(f"{now:.3f},{_lag(consumer)}\n")
                    lags.flush()
        finally:
            consumer.close()


if __name__ == "__main__":
    main()
