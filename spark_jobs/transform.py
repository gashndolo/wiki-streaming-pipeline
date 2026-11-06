from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_jobs.schemas import EVENT_SCHEMA

EARLIEST_PLAUSIBLE = "2001-01-01"


def parse_events(bronze: DataFrame) -> DataFrame:
    """Parse raw JSON and attach validation_error (null when the row is valid)."""
    parsed = bronze.select(
        "kafka_partition",
        "kafka_offset",
        F.col("ingested_at").alias("bronze_ingested_at"),
        "raw_json",
        F.from_json("raw_json", EVENT_SCHEMA).alias("e"),
    ).withColumn("event_time", F.timestamp_seconds(F.col("e.timestamp")))

    error = (
        F.when(F.col("raw_json").isNull()
               | F.get_json_object("raw_json", "$").isNull(), F.lit("unparseable_json"))
        .when(F.col("e").isNull(), F.lit("unparseable_json"))
        .when(F.col("e.meta.id").isNull(), F.lit("missing_event_id"))
        .when(F.col("e.wiki").isNull(), F.lit("missing_wiki"))
        .when(F.col("e.type").isNull(), F.lit("missing_type"))
        .when(F.col("e.timestamp").isNull(), F.lit("missing_timestamp"))
        .when(F.col("event_time") < F.lit(EARLIEST_PLAUSIBLE).cast("timestamp"),
              F.lit("implausible_time_past"))
        .when(F.col("event_time") > F.current_timestamp() + F.expr("INTERVAL 1 DAY"),
              F.lit("implausible_time_future"))
    )
    return parsed.withColumn("validation_error", error)


def to_silver(parsed: DataFrame) -> DataFrame:
    return parsed.where(F.col("validation_error").isNull()).select(
        F.col("e.meta.id").alias("event_id"),
        "event_time",
        F.to_date("event_time").alias("event_date"),
        F.col("e.wiki").alias("wiki"),
        F.col("e.type").alias("event_type"),
        F.col("e.namespace").alias("namespace"),
        F.col("e.title").alias("title"),
        F.col("e.user").alias("user_name"),
        F.coalesce(F.col("e.bot"), F.lit(False)).alias("is_bot"),
        F.coalesce(F.col("e.minor"), F.lit(False)).alias("is_minor"),
        F.col("e.id").alias("rc_id"),
        F.col("e.length.old").alias("old_length"),
        F.col("e.length.new").alias("new_length"),
        (F.col("e.length.new") - F.col("e.length.old")).alias("byte_delta"),
        F.col("e.server_name").alias("server_name"),
        "kafka_partition",
        "kafka_offset",
        "bronze_ingested_at",
        F.current_timestamp().alias("processed_at"),
    )


def to_quarantine(parsed: DataFrame) -> DataFrame:
    return parsed.where(F.col("validation_error").isNotNull()).select(
        "kafka_partition",
        "kafka_offset",
        "raw_json",
        F.col("validation_error").alias("reason"),
        F.current_timestamp().alias("quarantined_at"),
    )