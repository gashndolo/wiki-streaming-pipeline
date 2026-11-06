import logging

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from spark_jobs import config

log = logging.getLogger("gold")


def ensure_tables(spark):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{config.GOLD_MINUTE_PATH}` (
            window_start TIMESTAMP, window_end TIMESTAMP, wiki STRING,
            events BIGINT, bot_events BIGINT, human_events BIGINT,
            edit_events BIGINT, new_page_events BIGINT, log_events BIGINT,
            window_date DATE, updated_at TIMESTAMP
        ) USING DELTA PARTITIONED BY (window_date)""")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{config.GOLD_PAGE_PATH}` (
            window_start TIMESTAMP, window_end TIMESTAMP, wiki STRING, title STRING,
            edits BIGINT, net_bytes BIGINT,
            window_date DATE, updated_at TIMESTAMP
        ) USING DELTA PARTITIONED BY (window_date)""")


def make_upsert(path: str, keys: list[str], batch_filter: str | None = None):
    """foreachBatch function: MERGE the micro-batch's current window totals into Delta."""
    condition = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)

    def upsert(batch_df, batch_id: int):
        batch_df.persist()
        try:
            df = batch_df.where(batch_filter) if batch_filter else batch_df
            df = df.withColumn("updated_at", F.current_timestamp())
            if df.isEmpty():
                return
            (DeltaTable.forPath(df.sparkSession, path).alias("t")
             .merge(df.alias("s"), condition)
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
        finally:
            batch_df.unpersist()

    return upsert


def start_queries(spark):
    ensure_tables(spark)

    events = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", config.GOLD_MAX_FILES)
        .load(config.SILVER_PATH)
        .where(F.col("wiki") != config.TEST_WIKI)
        .withWatermark("event_time", config.GOLD_WATERMARK)
    )

    per_minute = (
        events.groupBy(F.window("event_time", "1 minute").alias("w"), "wiki")
        .agg(
            F.count("*").alias("events"),
            F.sum(F.col("is_bot").cast("long")).alias("bot_events"),
            F.sum(F.when(F.col("event_type") == "edit", 1).otherwise(0)).alias("edit_events"),
            F.sum(F.when(F.col("event_type") == "new", 1).otherwise(0)).alias("new_page_events"),
            F.sum(F.when(F.col("event_type") == "log", 1).otherwise(0)).alias("log_events"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "wiki",
            F.col("events").cast("long"),
            F.col("bot_events").cast("long"),
            (F.col("events") - F.col("bot_events")).cast("long").alias("human_events"),
            F.col("edit_events").cast("long"),
            F.col("new_page_events").cast("long"),
            F.col("log_events").cast("long"),
            F.to_date(F.col("w.start")).alias("window_date"),
        )
    )

    per_page = (
        events.where(F.col("event_type").isin("edit", "new"))
        .groupBy(F.window("event_time", "5 minutes").alias("w"), "wiki", "title")
        .agg(
            F.count("*").alias("edits"),
            F.sum(F.coalesce(F.col("byte_delta"), F.lit(0))).alias("net_bytes"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "wiki",
            "title",
            F.col("edits").cast("long"),
            F.col("net_bytes").cast("long"),
            F.to_date(F.col("w.start")).alias("window_date"),
        )
    )

    def start(df, path, keys, name, checkpoint, batch_filter=None):
        return (
            df.writeStream.outputMode("update")
            .foreachBatch(make_upsert(path, keys, batch_filter))
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime=config.GOLD_TRIGGER)
            .queryName(name)
            .start()
        )

    return [
        start(per_minute, config.GOLD_MINUTE_PATH, ["window_start", "wiki"],
              "gold_edits_per_minute", config.GOLD_MINUTE_CHECKPOINT),
        start(per_page, config.GOLD_PAGE_PATH, ["window_start", "wiki", "title"],
              "gold_edits_by_page_5min", config.GOLD_PAGE_CHECKPOINT,
              batch_filter="edits >= 2"),
    ]