"""Fill the SQLite database with ~10 minutes of demo data, no Kafka needed.

Handy for previewing the dashboard or working on it offline:
    python seed_db.py && streamlit run dashboard.py
"""
import os
from datetime import datetime, timedelta, timezone

import config
import storage
from alerts import AlertDetector
from data_generator import FeedbackGenerator
from sentiment import analyze

N, RATE = 1800, 3   # messages, messages per second


def main():
    if os.path.exists(config.DB_PATH):
        os.remove(config.DB_PATH)
    conn = storage.connect()
    gen = FeedbackGenerator(seed=11)
    detector = AlertDetector(config.ALERT_WINDOW_SEC, config.ALERT_MIN_MSGS,
                             config.ALERT_NEG_RATIO, config.ALERT_COOLDOWN_SEC)
    start = datetime.now(timezone.utc) - timedelta(seconds=N / RATE)
    alerts = 0
    for i in range(N):
        when = start + timedelta(seconds=i / RATE)
        record, _ = gen.next()
        record["timestamp"] = when.isoformat(timespec="seconds")
        result = analyze(record["text"])
        storage.insert_feedback(conn, {**record, **result})
        alert = detector.add(record["route"], result["sentiment"], result["topic"], now=when.timestamp())
        if alert:
            alert["timestamp"] = record["timestamp"]
            storage.insert_alert(conn, alert)
            alerts += 1
    conn.commit()
    print(f"seeded {N} messages and {alerts} alerts into {config.DB_PATH}")


if __name__ == "__main__":
    main()
