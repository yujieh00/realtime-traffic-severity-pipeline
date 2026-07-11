"""
Kafka producer that replays the collision sample as a real-time sensor feed.

Every second it reads a random batch of 50-100 collisions (in chronological
order, wrapping around at the end so the demo never stops), stamps each record
with the current epoch time as `event_time`, and publishes it as JSON to the
Kafka topic the Spark streaming job consumes.

Run (after `docker compose up -d`):
    python src/producer.py
"""

import datetime as dt
import random
from json import dumps
from time import sleep

import pandas as pd
from kafka import KafkaProducer

from config import (
    COLLISIONS_CSV,
    KAFKA_BOOTSTRAP,
    MAX_BATCH,
    MIN_BATCH,
    SEND_INTERVAL,
    TOPIC_INCOMING,
)


def connect_producer():
    """Create a KafkaProducer that serialises dicts to JSON bytes."""
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda x: dumps(x).encode("utf-8"),
    )


def load_records():
    """Load the sample once as a list of dicts (cheap O(1) slicing in the loop)."""
    df = pd.read_csv(COLLISIONS_CSV, dtype=str, keep_default_na=False)
    records = df.to_dict(orient="records")
    print(f"Loaded {len(records):,} rows from {COLLISIONS_CSV}")
    print("Columns:", list(df.columns))
    return records


def next_batch(records, pointer, size):
    """Return (batch, new_pointer), wrapping around at the end of the list.

    Pure and side-effect free so it can be unit-tested without Kafka.
    """
    total = len(records)
    batch = records[pointer:pointer + size]
    if len(batch) < size:                       # ran past the end -> wrap
        remaining = size - len(batch)
        batch = batch + records[:remaining]
        pointer = remaining
    else:
        pointer += size
    if pointer >= total:
        pointer = 0
    return batch, pointer


def main():
    records = load_records()
    pointer = 0

    producer = connect_producer()
    print(f"Publishing to topic '{TOPIC_INCOMING}' at {KAFKA_BOOTSTRAP} "
          f"({MIN_BATCH}-{MAX_BATCH} rows/sec). Ctrl-C to stop.")

    batch_no = 0
    try:
        while True:
            size = random.randint(MIN_BATCH, MAX_BATCH)
            batch, pointer = next_batch(records, pointer, size)

            # One event_time per batch: every row that "arrives" this second
            # shares it. This drives the Spark watermark and tumbling windows.
            event_time = int(dt.datetime.now().timestamp())
            for row in batch:
                message = dict(row)
                message["event_time"] = event_time
                producer.send(TOPIC_INCOMING, value=message)

            producer.flush()
            batch_no += 1
            print(f"batch {batch_no:>4} | rows={len(batch):>3} | "
                  f"event_time={event_time} | pointer={pointer}")
            sleep(SEND_INTERVAL)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.close()
        print("Producer connection closed.")


if __name__ == "__main__":
    main()
