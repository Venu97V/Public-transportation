"""Shared settings. Override with environment variables if needed."""
import os

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DB_PATH = os.getenv("DB_PATH", "sentiment.db")

# Kafka topics
TOPIC_RAW = "transport.feedback.raw"            # producer  -> processor
TOPIC_ANALYZED = "transport.feedback.analyzed"  # processor -> sink
TOPIC_ALERTS = "transport.alerts"               # processor -> sink
PARTITIONS = 3

# Alert rule: raise an alert when, in the last WINDOW seconds, a route has at
# least MIN_MSGS messages and at least NEG_RATIO of them are negative.
ALERT_WINDOW_SEC = 30
ALERT_MIN_MSGS = 15
ALERT_NEG_RATIO = 0.70
ALERT_COOLDOWN_SEC = 45
