# Public Transportation Sentiment Analysis Using Apache Kafka

A real-time pipeline that streams passenger feedback through Apache Kafka, classifies each
message as positive, neutral or negative, tags what it is about (delays, crowding, cleanliness...),
raises alerts when a route suddenly gets a wave of complaints, and shows everything on a live dashboard.

## Architecture

```
 feedback source          Kafka topics                      consumers                  storage / UI
┌──────────────┐   ┌──────────────────────────┐   ┌─────────────────────────┐
│ producer.py  │──▶│ transport.feedback.raw   │──▶│ processor.py            │
│ (synthetic   │   │ 3 partitions, key=route  │   │  - VADER + transport    │
│  stream or   │   └──────────────────────────┘   │    lexicon (sentiment)  │
│  CSV replay) │                                  │  - keyword topic tagger │
└──────────────┘   ┌──────────────────────────┐   │  - sliding-window alert │
                   │ transport.feedback.      │◀──│    rule                 │
                   │ analyzed                 │   └─────────────────────────┘
                   ├──────────────────────────┤
                   │ transport.alerts         │◀── (alerts)
                   └────────────┬─────────────┘
                                ▼
                         sink.py ──▶ SQLite (WAL) ──▶ dashboard.py (Streamlit + Plotly)
```

| File | Role |
|---|---|
| `docker-compose.yml` | Single-node Kafka (KRaft mode, no ZooKeeper) and an optional Kafka UI on port 8080 |
| `create_topics.py` | Creates the three topics with 3 partitions each |
| `data_generator.py` | Synthetic feedback with simulated complaint bursts |
| `producer.py` | Publishes feedback to `transport.feedback.raw` (or replays a CSV) |
| `sentiment.py` | Sentiment (VADER + custom transport lexicon) and topic classification |
| `alerts.py` | Sliding-window rule: route flagged when >= 70% of its last 30 s of messages are negative |
| `processor.py` | Consumer + producer: raw -> analyzed, plus alerts |
| `sink.py` | Second consumer group: analyzed + alerts -> SQLite |
| `dashboard.py` | Live dashboard |
| `evaluate.py` | Accuracy, confusion matrix, precision/recall/F1 |
| `seed_db.py` | Fills the database with demo data so the dashboard can be viewed without Kafka |
| `test_pipeline.py` | Offline unit tests |

## Setup

Requirements: Python 3.9+, Docker Desktop.

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
docker compose up -d               # starts Kafka (and Kafka UI at http://localhost:8080)
python create_topics.py
```

## Run (four terminals, activate the venv in each)

```bash
python processor.py                # 1. sentiment processor
python sink.py                     # 2. writes results to SQLite
streamlit run dashboard.py         # 3. dashboard at http://localhost:8501
python producer.py                 # 4. start the feedback stream (Ctrl+C to stop)
```

Useful producer options: `--rate 10` (messages/sec), `--count 500`, `--no-incidents`,
`--csv file.csv --text-col text` (replay a real dataset).

Without Docker you can still preview the dashboard: `python seed_db.py && streamlit run dashboard.py`.
Run the tests with `python -m unittest -v`.

## Demo script for your presentation

1. Open Kafka UI (localhost:8080) and show the three topics and their 3 partitions.
2. Start processor, sink, dashboard, then the producer. Watch messages flow in all terminals.
3. Wait for an incident: a route's complaints spike, `ALERT` prints in the processor terminal and
   appears in the dashboard's Alerts panel.
4. **Scaling:** start a second `python processor.py`. Kafka rebalances: each copy now handles some
   of the 3 partitions (the `[p0]`, `[p1]`, `[p2]` prefixes in the log show which).
5. **Fault tolerance:** stop the processor, let the producer run for 20 s, restart the processor.
   It resumes from its committed offset and processes everything it missed, nothing lost.
6. **Replay:** in Kafka UI, show the `sqlite-sink` consumer group and its lag. A brand-new consumer
   group can re-read all history from the raw topic.

## Kafka concepts this project demonstrates

| Concept | Where |
|---|---|
| Topics and partitions | `config.py`, `create_topics.py`: 3 partitions per topic |
| Message keys | `producer.py`: key = route, so one route's messages stay in order within a partition |
| Consumer groups and horizontal scaling | `processor.py`: copies share the group `sentiment-processor` |
| Independent consumers | `sink.py` uses group `sqlite-sink`, so it gets its own copy of the analyzed stream |
| Offsets and delivery guarantee | At-least-once. `sink.py` is idempotent (`INSERT OR IGNORE` on message id) so duplicates are harmless |
| Stream-to-stream processing | `processor.py` reads one topic, writes two |
| Decoupling | Producer, processor, sink and dashboard know nothing about each other, only about topics |

## Honest evaluation: what to put in your report

- `python evaluate.py` reports 100% on the synthetic data. **Do not present that as the model's accuracy:**
  the messages were written from vocabulary the model already knows.
- For a real number, evaluate on a labelled dataset (for example the public *Twitter US Airline Sentiment*
  data on Kaggle, which is the closest freely available match to transport complaints):
  `python evaluate.py --csv Tweets.csv --text-col text --label-col airline_sentiment`.
  Expect a figure well below 100%. Report it together with the confusion matrix.
- On a small hand-written set of 11 realistic sentences the model got 10 right; the miss was sarcasm.

**Known limitations** (good material for the "limitations and future work" section):
rule-based sentiment misses sarcasm and can be fooled by idioms; English only (no Hinglish or
other languages); the topic tagger uses keywords, so a message can be tagged with the wrong topic;
the SQLite sink is single-writer; one Kafka broker means no replication.

**Future work:** replace VADER with a fine-tuned transformer (e.g. a multilingual BERT) or call a
hosted LLM; ingest live data from the X, Reddit or Google Play review APIs; use Spark Structured
Streaming for windowed aggregation; send results to Elasticsearch and Grafana; run a multi-broker
cluster with replication factor 3.

## Mapping to a project report

1. Abstract and problem statement (your text)
2. Literature survey: Kafka, stream processing, VADER/lexicon-based vs ML sentiment analysis
3. System architecture: the diagram above
4. Implementation: one subsection per file in the table
5. Results: dashboard screenshots, evaluate.py output, alert demonstration, scaling and recovery demos
6. Conclusion, limitations, future work
