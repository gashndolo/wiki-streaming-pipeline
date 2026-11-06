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
| 2026-11-06 | 2 | Bronze integrity after crash test | 66,806 rows; 0 duplicates; 0 gaps; min_offset 0 on all 3 partitions | `scripts/verify_bronze.py` after `kill -9` and restart |
| 2026-11-06 | 2 | Partition distribution (key = wiki) | P0 25,708 (38.5%), P1 34,783 (52.1%), P2 4,812 (7.2%) | per-partition counts in bronze |
| 2026-11-06 | 2 | Bronze files and size | 144 files, 22.3 MB (~155 KB per file, ~334 bytes per row) | `DESCRIBE DETAIL` in verify script |
| 2026-11-06 | 2 | Post-crash latency (not steady state) | p50 24.2 s, p95 138.3 s; window inflated by recovery batch | verify script, window includes catch-up |
| 2026-11-06 | 2 | Crash test: recovery | `kill -9` mid-stream; no bronze progress for ~2 min 56 s (last batch logged 21:04:40, first recovered batch 21:07:36); first batch after restart read 7,688 rows in 26.2 s; batch numbering continued from checkpoint (42 -> 43) | bronze log |
| 2026-11-06 | 2 | Bronze micro-batch performance | ~1,500 rows per 30 s batch, processed in ~5.8-6.1 s (~260 rows/s) against ~50 rows/s input; peak 293 rows/s on the 7,688-row recovery batch | bronze log `processed_rate`, `batch_ms` |
| 2026-11-06 | 2 | Steady-state latency, Kafka to bronze batch start | p50 14.9 s, p95 28.7 s (n=13,237 rows over 5 min, ~44 events/s) | `verify_bronze.py`, 6 min after crash recovery; `ingested_at` is batch start, excludes ~6 s commit time |


<!--
>== Integrity per Kafka partition ==
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

2026-11-06 21:04:40,641 INFO bronze: batch=42 rows=1566 input_rate=49.7/s processed_rate=258.0/s batch_ms=6070
Killed
2026-11-06 21:07:36,788 INFO bronze: batch=43 rows=7688 input_rate=0.0/s processed_rate=292.9/s batch_ms=26243
2026-11-06 21:07:43,530 INFO bronze: batch=44 rows=1417 input_rate=53.8/s processed_rate=139.4/s batch_ms=10165
2026-11-06 21:08:08,556 INFO bronze: batch=45 rows=1526 input_rate=54.4/s processed_rate=262.5/s batch_ms=5814
-->