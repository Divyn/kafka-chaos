"""Compare what the broker acknowledged with what the consumer received.

Run after a chaos run: python3 check.py

The producer's log holds the events the broker acknowledged. The consumer's log
holds the events that arrived. Anything in the first set and not in the second
was acknowledged by Kafka and never read, which is the only loss that matters.
"""


def _read(path: str, column: int, only_ok: bool = False) -> set[str]:
    """Read one column of order numbers out of a log file."""
    values = set()
    with open(path) as log:
        for line in log:
            if not line.strip():
                continue
            if only_ok and ",ok," not in line:
                continue
            values.add(line.split(",")[column])
    return values


def main() -> None:
    acknowledged = _read("producer.log", 2, only_ok=True)
    received = _read("consumer.log", 1)
    missing = acknowledged - received
    print(f"acknowledged {len(acknowledged)}, received {len(received)}, "
          f"never arrived {len(missing)}")
    if missing:
        print("missing: " + ", ".join(sorted(missing)[:20]))


if __name__ == "__main__":
    main()
