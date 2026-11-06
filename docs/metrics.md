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
| 2026-11-06 | 3 | Row counts (snapshot while streaming) | bronze 255,861; silver 255,850; quarantine 6 (counts taken at slightly different moments) | `verify_silver.py` |
| 2026-11-06 | 3 | Dedupe injection test (2 runs) | each run sent one valid event twice; silver holds each exactly once | `inject_test_events.py` |
| 2026-11-06 | 3 | Quarantine routing | 2x implausible_time_past, 2x missing_type correct; malformed JSON mislabeled missing_event_id (bug found, fix pending) | `verify_silver.py` |
| 2026-11-06 | 3 | Silver size | 180 files, 20.6 MB for 255,850 rows (~80 bytes/row, ~115 KB/file) | `DESCRIBE DETAIL` |
| 2026-11-06 | 3 | Event time to silver latency | p50 46.2 s, p95 60.1 s (n=15,062 rows in 5 min) | `verify_silver.py`; includes two 30 s triggers |
| 2026-11-06 | 3 | Silver crash test: recovery | `kill -9` on silver; ~3 min 17 s between last batch before kill (22:29:40) and first after restart (22:32:57); batch numbering continued (silver 48 -> 49, quarantine 46 -> 47) | silver log |
| 2026-11-06 | 3 | Dedupe state restored after crash | state_rows 52,083 before kill -> 52,597 after restart (not reset); dropped_by_watermark 0 throughout | silver log |
| 2026-11-06 | 3 | Silver restart warm-up | First batch after restart 37.7 s vs ~7 s typical | silver log `batch_ms` |
| 2026-11-06 | 3 | Silver duplicates after crash recovery | 0 duplicate event_ids after kill -9 and restart (silver 315,497 rows, snapshot while streaming) | `verify_silver.py` |
| 2026-11-06 | 3 | Silver files and size (later snapshot) | 308 files, 26.2 MB (~85 KB/file, ~83 bytes/row) | `DESCRIBE DETAIL` |
| 2026-11-06 | 3 | Event time to silver latency (second run) | p50 46.1 s, p95 59.4 s (n=10,484), consistent with first run | `verify_silver.py` |
| 2026-11-06 | 4 | Compaction: bronze | 846 files -> 1 (-99.9%); active size 138.5 -> 134.3 MB; average file ~164 KB -> 134 MB | `spark_jobs.maintenance` (OPTIMIZE); one daily partition |
| 2026-11-06 | 4 | Compaction: silver | 484 files -> 1 (-99.8%); active size 35.0 -> 30.5 MB (-13%); average file ~72 KB -> 30.5 MB | `spark_jobs.maintenance` (OPTIMIZE) |
| 2026-11-06 | 4 | Compaction: gold | Not measured yet (gold tables were empty) | pending |
| 2026-11-07 | 4 | Incident: Spark 4.1.0 Kafka-source NPE in progress metrics | Bronze query crashed with NullPointerException in KafkaMicroBatchStream.metrics (known upstream regression in the 4.1 line); no data damage; pinned Spark/Delta to 4.0.1 and rebuilt all layers from Kafka | pipeline log, upstream issue SPARK-57438 |
| 2026-11-07 | 4 | Gold verification | Not completed (environment issues: Spark 4.1 Kafka NPE, then PySpark install segfault); gold reconciliation and late-data tests not run | n/a |