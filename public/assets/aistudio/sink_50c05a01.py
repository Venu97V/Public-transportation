"""Sink: analyzed feedback + alerts (Kafka) -> SQLite, which the dashboard reads."""
import json
import signal
import time

from confluent_kafka import Consumer

import config
import storage

running = True


def stop(*_):
    global running
    running = False


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP,
        "group.id": "sqlite-sink",         # different group => gets its own copy of every message
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([config.TOPIC_ANALYZED, config.TOPIC_ALERTS])
    conn = storage.connect()
    print(f"sink writing to {config.DB_PATH} ...")

    pending, last_commit, total = 0, time.time(), 0
    try:
        while running:
            msg = consumer.poll(0.5)
            if msg is not None and not msg.error():
                try:
                    payload = json.loads(msg.value())
                    if msg.topic() == config.TOPIC_ALERTS:
                        storage.insert_alert(conn, payload)
                    else:
                        storage.insert_feedback(conn, payload)
                    pending += 1
                    total += 1
                except (ValueError, KeyError) as exc:
                    print(f"skipping bad message at offset {msg.offset()}: {exc}")
            elif msg is not None:
                print(f"consumer error: {msg.error()}")

            # batch commits: far fewer disk writes than one per message
            if pending and (pending >= 50 or time.time() - last_commit > 1):
                conn.commit()
                pending, last_commit = 0, time.time()
                print(f"stored {total} rows", end="\r")
    finally:
        conn.commit()
        conn.close()
        consumer.close()
        print(f"\nsink stopped, {total} rows stored")


if __name__ == "__main__":
    main()
