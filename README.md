# Wikipedia Edits Streaming Pipeline

A real-time streaming pipeline that ingests Wikimedia's live edit feed into
Kafka, processes it with Spark Structured Streaming into Delta Lake tables
(bronze, silver, gold), and serves near-real-time aggregates on a dashboard:
edits per minute by wiki, bot vs human activity, and the most-edited pages.

**Stack:** Python · Apache Kafka · Spark Structured Streaming · Delta Lake · Streamlit · Docker · GitHub Actions

**Status:** in progress (Phase 0: setup).
