# Metrics log

Every number measured during the build goes here, with how it was measured.
These become the README results and the resume bullets.

| Date | Phase | Metric | Value | How measured |
|------|-------|--------|-------|--------------|
| 2026-11-06 | 0 | Docker memory available | 7.16 GiB | `docker info` |
| 2026-11-06 | 0 | Baseline event rate | 1,005 events / 30 s = 33.5 events/s (~2.9M/day) | curl count of `data:` lines |
| 2026-11-06 | 1 | Producer run: events published | 8,623 produced, 8,623 delivered, 0 invalid, 0 delivery errors, 0 unflushed | final producer log line |
| 2026-11-06 | 1 | Bad-feed test (Test B) | 0 produced, clean exit, 0 unflushed | `WIKI_STREAM_URL` pointed at a nonexistent stream |
| 2026-11-06 | 1 | Kafka outage test (Test A) | Broker stopped mid-run, then restarted; delivery_errors stayed 0 | `docker compose stop kafka`, wait, `docker compose start kafka` during a producer run |
| 2026-11-06 | 1 | Producer sustained rate | 35.6 events/s (matches the 33.5/s baseline), 0 reconnects, 0 invalid | producer `rate=` log line, 10 s window |
| 2026-11-06 | 1 | Reconnect backoff (Test B) | Wait time grew on each failed attempt; clean exit on Ctrl+C | `WIKI_STREAM_URL` pointed at a nonexistent stream |


== Integrity per Kafka partition ==
+---------------+-----+----------------+----------+----------+----------+----+
|kafka_partition|rows |distinct_offsets|min_offset|max_offset|duplicates|gaps|
+---------------+-----+----------------+----------+----------+----------+----+
|0              |17396|17396           |0         |17395     |0         |0   |
|1              |23230|23230           |0         |23229     |0         |0   |
|2              |3414 |3414            |0         |3413      |0         |0   |
+---------------+-----+----------------+----------+----------+----------+----+

TOTAL rows=45535 duplicates=0 gaps=0

== Files == numFiles=108 size_mb=14.8

== Kafka -> bronze latency (rows ingested in last 5 min) ==
n=12154 p50=15.2s p95=28.5s