"""Create the three Kafka topics (safe to run more than once)."""
from confluent_kafka.admin import AdminClient, NewTopic

import config

admin = AdminClient({"bootstrap.servers": config.BOOTSTRAP})
topics = [
    NewTopic(name, num_partitions=config.PARTITIONS, replication_factor=1)
    for name in (config.TOPIC_RAW, config.TOPIC_ANALYZED, config.TOPIC_ALERTS)
]

for name, future in admin.create_topics(topics).items():
    try:
        future.result()
        print(f"created   {name} ({config.PARTITIONS} partitions)")
    except Exception as exc:  # most common: topic already exists
        print(f"skipped   {name}: {exc}")
