"""Send order events to the cluster and record what the broker acknowledged.

Run on the Docker host: python3 producer.py [count] [interval_ms]

Every line in producer.log is one event: the time the broker acknowledged it,
the order number, the partition and offset it landed on, and the error if it
was never acknowledged.
"""

import json
import sys
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = "localhost:19092,localhost:19093,localhost:19094"
TOPIC = "orders"
PARTITIONS = 3
REPLICATION = 3
MIN_ISR = 2


def _create_topic() -> None:
    """Create the topic if it is not there, with three copies of every partition."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    topic = NewTopic(
        TOPIC,
        num_partitions=PARTITIONS,
        replication_factor=REPLICATION,
        config={"min.insync.replicas": str(MIN_ISR)},
    )
    for future in admin.create_topics([topic]).values():
        try:
            future.result()
            print(f"created topic {TOPIC}: {PARTITIONS} partitions, {REPLICATION} copies")
        except Exception as error:
            print(f"topic: {error}")


def main() -> None:
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 3000
    interval = (int(sys.argv[2]) if len(sys.argv) > 2 else 20) / 1000
    _create_topic()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 1000000,
        "message.timeout.ms": 15000,
        "linger.ms": 5,
    })

    acked = 0
    failed = 0
    with open("producer.log", "w") as log:
        def report(error, message) -> None:
            nonlocal acked, failed
            if error is not None:
                failed += 1
                log.write(f"{time.time():.3f},error,{error}\n")
            else:
                acked += 1
                log.write(
                    f"{time.time():.3f},ok,{message.key().decode()},"
                    f"{message.partition()},{message.offset()}\n"
                )
            log.flush()

        start = time.time()
        for order in range(1, count + 1):
            value = json.dumps({"order": order, "at": round(time.time(), 3)})
            producer.produce(TOPIC, key=str(order), value=value, callback=report)
            producer.poll(0)
            if order % 500 == 0:
                print(f"{order} sent, {acked} acknowledged, {failed} failed")
            time.sleep(interval)
        producer.flush(30)

    print(f"done in {time.time() - start:.1f} s: {count} sent, {acked} acknowledged, {failed} failed")


if __name__ == "__main__":
    main()
