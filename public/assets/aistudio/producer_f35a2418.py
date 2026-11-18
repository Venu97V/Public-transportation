"""Publish passenger feedback to Kafka.

  python producer.py                         # synthetic stream, 3 msgs/sec
  python producer.py --rate 10 --count 500   # faster, stop after 500
  python producer.py --csv tweets.csv --text-col text   # replay a real dataset
"""
import argparse
import csv
import json
import time

from confluent_kafka import Producer

import config
from data_generator import FeedbackGenerator, make_record


def csv_records(path, text_col):
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            text = (row.get(text_col) or "").strip()
            if text:
                yield make_record(text, source="csv-replay")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=3, help="messages per second")
    ap.add_argument("--count", type=int, default=0, help="stop after N messages (0 = run forever)")
    ap.add_argument("--csv", help="replay this CSV instead of generating data")
    ap.add_argument("--text-col", default="text", help="CSV column holding the feedback text")
    ap.add_argument("--no-incidents", action="store_true", help="disable simulated complaint bursts")
    args = ap.parse_args()

    if args.csv:
        stream = csv_records(args.csv, args.text_col)
    else:
        gen = FeedbackGenerator(incidents=not args.no_incidents)
        stream = (gen.next()[0] for _ in iter(int, 1))   # endless; drops the hidden label

    producer = Producer({"bootstrap.servers": config.BOOTSTRAP, "linger.ms": 20})

    def on_delivery(err, msg):
        if err:
            print(f"delivery failed: {err}")

    sent = 0
    try:
        for record in stream:
            # key = route -> all messages of a route land in the same partition,
            # so their relative order is preserved.
            producer.produce(
                config.TOPIC_RAW,
                key=record["route"],
                value=json.dumps(record),
                on_delivery=on_delivery,
            )
            producer.poll(0)
            sent += 1
            print(f"[{sent}] {record['route']:<16} {record['text']}")
            if args.count and sent >= args.count:
                break
            time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10)
        print(f"\nproducer stopped after {sent} messages")


if __name__ == "__main__":
    main()
