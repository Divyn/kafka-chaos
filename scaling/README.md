# More Kafka consumers or more threads?

`scaling.py` reads the same 50,000 order events 6 ways and times each one. The handler
writes one small file per event and calls fsync, so it waits on the disk for every
event, like a database write.

    docker compose up -d          # in the folder above: 3 brokers, Kafka 3.9.0
    pip install confluent-kafka
    python3 scaling.py fill       # once: 50,000 events into a 12 partition topic
    python3 scaling.py race 3     # every setup, 3 passes

Each setup starts its consumers at once in a new consumer group, and the clock runs
until 50,000 different events are handled. Offsets are not committed, so every pass
reads the same events. Events read again during the group join are counted as "read
twice" and not as new.

Results in `results_scaling.log` (2 vCPU Vultr server, 2026-11-17, median of 3 passes):

| Setup | Seconds | Events per second |
| --- | --- | --- |
| 1 consumer | 31.21 | 1,602 |
| 3 consumers | 17.05 | 2,933 |
| 1 consumer, 4 threads | 13.35 | 3,746 |
| 6 consumers | 11.98 | 4,173 |
| 3 consumers, 4 threads each | 10.38 | 4,819 |
| 1 consumer, 16 threads | 8.50 | 5,881 |

With a handler that waits, threads wait together, so 1 consumer with 16 threads beat
6 consumers. With threads, events from one partition can finish out of order.
