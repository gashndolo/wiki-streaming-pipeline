import logging

from spark_jobs import config
from spark_jobs.common import get_spark
from spark_jobs.transform import parse_events, to_quarantine, to_silver

log = logging.getLogger("silver")


def start_queries(spark):
    bronze = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", config.SILVER_MAX_FILES)
        .load(config.BRONZE_PATH)
    )
    parsed = parse_events(bronze)

    deduped = (
        to_silver(parsed)
        .withWatermark("event_time", config.DEDUPE_WATERMARK)
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    silver_q = (
        deduped.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.SILVER_CHECKPOINT)
        .partitionBy("event_date")
        .trigger(processingTime=config.SILVER_TRIGGER)
        .queryName("silver_wiki_events")
        .start(config.SILVER_PATH)
    )
    quarantine_q = (
        to_quarantine(parsed).writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.QUARANTINE_CHECKPOINT)
        .trigger(processingTime=config.SILVER_TRIGGER)
        .queryName("quarantine_wiki_events")
        .start(config.QUARANTINE_PATH)
    )
    return [silver_q, quarantine_q]


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = get_spark("silver_wiki_events", driver_memory="1g")
    queries = start_queries(spark)
    last = {q.name: -1 for q in queries}
    log.info("Silver and quarantine streams started")

    try:
        while all(q.isActive for q in queries):
            spark.streams.awaitAnyTermination(5)     # raises if a query failed
            for q in queries:
                p = q.lastProgress
                if p and p["batchId"] != last[q.name]:
                    last[q.name] = p["batchId"]
                    state = (p.get("stateOperators") or [{}])[0]
                    log.info("%s batch=%s rows_in=%s state_rows=%s "
                             "dropped_by_watermark=%s batch_ms=%s",
                             q.name, p["batchId"], p["numInputRows"],
                             state.get("numRowsTotal", "-"),
                             state.get("numRowsDroppedByWatermark", "-"),
                             p["durationMs"].get("triggerExecution"))
    except KeyboardInterrupt:
        log.info("Stopping streams...")
        for q in queries:
            q.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()