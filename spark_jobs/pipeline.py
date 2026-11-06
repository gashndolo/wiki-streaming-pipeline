import logging
import time

from delta.tables import DeltaTable

from spark_jobs import bronze, config, gold, silver
from spark_jobs.common import get_spark

log = logging.getLogger("pipeline")


def wait_for_delta_table(spark, path: str, timeout_s: int = 300):
    start = time.time()
    while not DeltaTable.isDeltaTable(spark, path):
        if time.time() - start > timeout_s:
            raise TimeoutError(f"Upstream table never appeared: {path}")
        time.sleep(2)


def log_progress(query, last: dict):
    p = query.lastProgress
    if not p or p["batchId"] == last.get(query.name):
        return
    last[query.name] = p["batchId"]
    state = (p.get("stateOperators") or [{}])[0]
    log.info("%-26s batch=%s rows_in=%s state_rows=%s dropped_by_watermark=%s batch_ms=%s",
             query.name, p["batchId"], p["numInputRows"],
             state.get("numRowsTotal", "-"),
             state.get("numRowsDroppedByWatermark", "-"),
             p["durationMs"].get("triggerExecution"))


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = get_spark("wiki_pipeline", driver_memory=config.PIPELINE_MEMORY, cores=4)

    queries = [bronze.build_query(spark)]
    wait_for_delta_table(spark, config.BRONZE_PATH)
    queries += silver.start_queries(spark)
    wait_for_delta_table(spark, config.SILVER_PATH)
    queries += gold.start_queries(spark)
    log.info("Started %d streaming queries: %s", len(queries), [q.name for q in queries])

    last: dict = {}
    try:
        while all(q.isActive for q in queries):
            spark.streams.awaitAnyTermination(5)      # raises if any query failed
            for q in queries:
                log_progress(q, last)
    except KeyboardInterrupt:
        log.info("Stopping all streams...")
        for q in queries:
            q.stop()
    except Exception:
        log.exception("A streaming query failed; stopping all queries")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()