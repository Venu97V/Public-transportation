"""Stream processor: raw feedback -> sentiment analysis -> analyzed topic (+ alerts).

Run several copies in separate terminals: because they share one consumer
group, Kafka splits the 3 partitions between them (horizontal scaling).
"""
import json
import signal

from confluent_kafka import Consumer, KafkaError, Producer

import config
from alerts import AlertDetector
from sentiment import analyze

running = True


def stop(*_):
    global running
    running = False


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP,
        "group.id": "sentiment-processor",
        "auto.offset.reset": "earliest",   # a new group starts from the beginning of the topic
    })
    producer = Producer({"bootstrap.servers": config.BOOTSTRAP, "linger.ms": 20})
    detector = AlertDetector(
        config.ALERT_WINDOW_SEC, config.ALERT_MIN_MSGS,
        config.ALERT_NEG_RATIO, config.ALERT_COOLDOWN_SEC,
    )
    consumer.subscribe([config.TOPIC_RAW])
    print(f"processor listening on {config.TOPIC_RAW} ...")

    processed = 0
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"consumer error: {msg.error()}")
                continue

            try:
                record = json.loads(msg.value())
                result = analyze(record["text"])
            except (ValueError, KeyError) as exc:   # skip malformed messages instead of crashing
                print(f"skipping bad message at offset {msg.offset()}: {exc}")
                continue

            analyzed = {**record, **result}
            producer.produce(config.TOPIC_ANALYZED, key=record["route"], value=json.dumps(analyzed))

            alert = detector.add(record["route"], result["sentiment"], result["topic"])
            if alert:
                producer.produce(config.TOPIC_ALERTS, key=alert["route"], value=json.dumps(alert))
                print(f"ALERT  {alert['route']}: {alert['negative_ratio']:.0%} negative "
                      f"over {alert['messages']} msgs ({alert['top_topic']})")

            producer.poll(0)
            processed += 1
            print(f"[p{msg.partition()}] {result['sentiment']:<8} {result['score']:+.2f} "
                  f"{result['topic']:<20} {record['text'][:60]}")
    finally:
        producer.flush(10)
        consumer.close()   # commits offsets and leaves the group cleanly
        print(f"\nprocessor stopped after {processed} messages")


if __name__ == "__main__":
    main()
