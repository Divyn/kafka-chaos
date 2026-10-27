# Chaos Testing a Kafka Broker: Kill One and Watch What Breaks

A three broker Kafka cluster, a producer sending order events, a consumer reading them, and a script that kills one broker and measures what the pipeline sees: how long acknowledgements stop, how long the partitions take to get a new leader, and whether any event goes missing.

Kafka's [replication docs](https://kafka.apache.org/documentation/#replication) say the data survives a broker failure. This repo shows what that costs while it happens, and what changes when two brokers fail instead of one.

## The cluster

Three brokers, each one a controller too, in KRaft mode. No ZooKeeper.

| Broker | Address on the host | Address inside the compose network |
| --- | --- | --- |
| `kafka-1` | `localhost:19092` | `kafka-1:9092` |
| `kafka-2` | `localhost:19093` | `kafka-2:9092` |
| `kafka-3` | `localhost:19094` | `kafka-3:9092` |

Each broker opens one extra port on the host, so the producer, the consumer and the chaos script can run on the host instead of inside Docker. Broker to broker traffic uses the internal names. The controller listener is `9093`.

Every broker gets a 256 MB heap (`KAFKA_HEAP_OPTS`), which is enough for this test and small enough to run three of them on one machine.

## The topic and the three settings that matter

`producer.py` creates the topic before it sends anything:

| Setting | Value | What it does |
| --- | --- | --- |
| `partitions` | 3 | One partition per broker, so a dead broker always owns some data |
| `replication_factor` | 3 | Every partition has a copy on all three brokers |
| `min.insync.replicas` | 2 | A write needs two copies to succeed |

The producer also runs with `acks=all`, which means it waits for every in-sync copy, and with idempotence on, so a retry cannot write the same event twice.

Those four numbers together are what makes the test interesting. With three copies and only one broker dead, two copies are still there, so writes keep working. With two brokers dead, only one copy is left, `min.insync.replicas` is no longer met, and writes stop with an error instead of an error being hidden.

## How the chaos script works

`chaos.py` runs on the Docker host as root. Each round it:

1. Reads the topic metadata and finds the partitions the victim broker leads.
2. Kills that broker's process with `SIGKILL`. It reads the process ID with `docker inspect -f "{{.State.Pid}}"`, the same way the [Docker Compose chaos monkey](https://github.com/Divyn/chaos-compose) does.
3. Watches for 25 seconds: the times the producer gets acknowledgements, and the moment every partition the victim led has a leader that is not the victim.
4. Prints one row.

### Why it kills the process and not the container

`restart: always` brings a crashed container back. `docker stop` counts as stopping it by hand, so Docker does not restart it and the broker never returns. Killing the process looks like a crash, so the restart policy runs and the broker comes back on its own.

## Requirements

- A Linux machine with Docker and Docker Compose. Tested on 2 vCPU, 3916 MB of memory and 75 GB of disk, Debian 13.7, Docker 26.1.5, Docker Compose 2.26.1, Kafka 3.9.0.
- Python 3 on the host with `confluent-kafka`:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

## Run it

```bash
git clone https://github.com/Divyn/kafka-chaos.git
cd kafka-chaos
docker compose up -d
sleep 45          # three brokers take a moment to form a cluster
```

Then, in three terminals:

```bash
# 1. the consumer
python3 consumer.py 200

# 2. the producer, 8000 events, one every 20 ms
python3 producer.py 8000 20

# 3. the chaos rounds, after the producer has been running for ten seconds
sudo python3 chaos.py 3 12
```

When the producer finishes, compare the two logs:

```bash
python3 check.py
```

## Reading the output

```
round  killed        stall  new leader  errors  partitions
1      kafka-3          6.57 s        7.9 s       0      1
```

| Column | Meaning |
| --- | --- |
| `killed` | The broker whose process was killed |
| `stall` | The longest gap with no acknowledgements, in seconds |
| `new leader` | Seconds until every partition the victim led had another leader |
| `errors` | Events the producer gave up on during the round |
| `partitions` | How many partitions the victim led before it was killed |

`check.py` prints three numbers: what the broker acknowledged, what the consumer received, and what was acknowledged but never received. The last one is the only loss that matters.

## Results from one run

From the machine above, in [output.txt](output.txt).

```
round  killed        stall  new leader  errors  partitions
1      kafka-3          6.57 s        7.9 s       0      1
2      kafka-1          6.18 s        8.6 s       0      2
3      kafka-3          6.00 s        8.1 s       0      1
```

- Acknowledgements stopped for 6.57, 6.18 and 6.00 seconds. That is the pause your service sees.
- `errors` is 0 in every round, and `check.py` found 7201 acknowledged events and 7201 received, with none missing.
- The stall is the same whether the killed broker led one partition or two.

Two brokers killed at the same time, with the producer still sending:

```
$ grep -c ",error," producer.log
245

$ grep -m1 ",error," producer.log | cut -d, -f3-
KafkaError{code=_MSG_TIMED_OUT,val=-192,str="Local: Message timed out"}
```

245 events were never acknowledged. Every one of them was reported to the producer, so the application can retry or tell someone. Nothing vanished silently.

## What to watch out for

**Six seconds is the pause, not the outage.** The broker takes longer to come back than the partition takes to get a new leader. Your consumer stops because the producer stopped, and the producer stops while it waits for the new leader.

**Two brokers down is a different failure.** With three copies and two brokers dead, only one copy is left. `min.insync.replicas=2` then rejects the write, and after `message.timeout.ms` the producer reports `Local: Message timed out`. If you run with `min.insync.replicas=1`, the write succeeds on that last copy and you lose the guarantee instead.

**The in-sync copy count does not drop at the moment of the crash.** Kafka keeps a dead follower in the in-sync list until the leader decides it has been gone too long (`replica.lag.time.max.ms`, 30 seconds by default). So the partition has fewer live copies than the ISR says for up to 30 seconds.

**Two of three controllers is still a majority, one is not.** Kill two brokers in this cluster and the remaining one cannot elect leaders or accept metadata changes. That is why writes stop rather than move to the last broker.

## Use it on your own cluster

1. Change `BOOTSTRAP` in all three Python files to your brokers.
2. Change `TOPIC` and the replication settings in `producer.py`.
3. Change `BROKERS` in `chaos.py` to your container names. The script kills them with `docker inspect` and `SIGKILL`, so it has to run on the machine that runs them.
4. Set `min.insync.replicas` to match the number of brokers you can afford to lose.

## Files

| File | What it does |
| --- | --- |
| `docker-compose.yml` | Three brokers in KRaft mode, one host port each |
| `producer.py` | Creates the topic, sends order events, logs every acknowledgement |
| `consumer.py` | Reads the events, writes each one down, records the lag every second |
| `chaos.py` | Kills a broker and measures the stall and the leader change |
| `check.py` | Compares what was acknowledged with what was received |
| `output.txt` | Real output from the run shown here |
