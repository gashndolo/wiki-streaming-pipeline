import logging

from pyspark.sql import functions as F

from spark_jobs import config
from spark_jobs.common import get_spark

log = logging.getLogger("bronze")


def build_query(spark):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", config.MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "true")
        .load()
    )

    bronze = raw.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.current_timestamp().alias("ingested_at"),
        F.to_date("timestamp").alias("ingest_date"),
    )

    return (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.BRONZE_CHECKPOINT)
        .partitionBy("ingest_date")
        .trigger(processingTime=config.BRONZE_TRIGGER)
        .queryName("bronze_wiki_recentchange")
        .start(config.BRONZE_PATH)
    )


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    spark = get_spark("bronze_wiki_recentchange")
    query = build_query(spark)
    log.info("Bronze stream started: %s -> %s", config.TOPIC, config.BRONZE_PATH)

    last_batch = -1
    try:
        while query.isActive:
            query.awaitTermination(5)          # short wait keeps Ctrl+C responsive
            progress = query.lastProgress
            if progress and progress["batchId"] != last_batch:
                last_batch = progress["batchId"]
                log.info("batch=%s rows=%s input_rate=%.1f/s processed_rate=%.1f/s "
                         "batch_ms=%s", progress["batchId"], progress["numInputRows"],
                         float(progress.get("inputRowsPerSecond") or 0.0),
                         float(progress.get("processedRowsPerSecond") or 0.0),
                         progress["durationMs"].get("triggerExecution"))
    except KeyboardInterrupt:
        log.info("Stopping bronze stream...")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()