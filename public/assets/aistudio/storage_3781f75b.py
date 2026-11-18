"""SQLite storage shared by the sink (writer) and the dashboard (reader)."""
import sqlite3

import pandas as pd

import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS feedback (
    id        TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    source    TEXT, mode TEXT, route TEXT, text TEXT,
    sentiment TEXT, score REAL, topic TEXT
);
CREATE INDEX IF NOT EXISTS idx_feedback_ts ON feedback(timestamp);
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL, route TEXT,
    negative_ratio REAL, messages INTEGER, top_topic TEXT,
    UNIQUE (timestamp, route)
);
"""


def connect(path=None):
    conn = sqlite3.connect(path or config.DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")   # lets the dashboard read while the sink writes
    conn.executescript(SCHEMA)
    return conn


def insert_feedback(conn, rec):
    # INSERT OR IGNORE makes the sink idempotent: Kafka gives at-least-once
    # delivery, so a message can occasionally arrive twice.
    conn.execute(
        "INSERT OR IGNORE INTO feedback VALUES (?,?,?,?,?,?,?,?,?)",
        (rec["id"], rec["timestamp"], rec.get("source"), rec.get("mode"),
         rec.get("route"), rec.get("text"), rec["sentiment"], rec["score"], rec["topic"]),
    )


def insert_alert(conn, alert):
    conn.execute(
        "INSERT OR IGNORE INTO alerts (timestamp, route, negative_ratio, messages, top_topic) "
        "VALUES (?,?,?,?,?)",
        (alert["timestamp"], alert["route"], alert["negative_ratio"],
         alert["messages"], alert["top_topic"]),
    )


def load_feedback(since=None, modes=None):
    """Feedback as a DataFrame; `since` is an ISO-8601 UTC string."""
    sql, params = "SELECT * FROM feedback WHERE 1=1", []
    if since:
        sql += " AND timestamp >= ?"
        params.append(since)
    if modes:
        sql += f" AND mode IN ({','.join('?' * len(modes))})"
        params += list(modes)
    with connect() as conn:
        df = pd.read_sql_query(sql + " ORDER BY timestamp", conn, params=params)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df


def load_alerts(limit=8):
    with connect() as conn:
        return pd.read_sql_query(
            "SELECT * FROM alerts ORDER BY timestamp DESC LIMIT ?", conn, params=[limit]
        )
