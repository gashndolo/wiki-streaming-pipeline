from pyspark.sql import functions as F

from spark_jobs import config
from spark_jobs.common import get_spark

TEST_WIKI = "pipeline_test"


def delta_detail(spark, path):
    d = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").select("numFiles", "sizeInBytes").first()
    return d["numFiles"], d["sizeInBytes"] / 1e6


def main():
    spark = get_spark("verify_silver", driver_memory="1g")
    bronze = spark.read.format("delta").load(config.BRONZE_PATH)
    silver = spark.read.format("delta").load(config.SILVER_PATH)
    quarantine = spark.read.format("delta").load(config.QUARANTINE_PATH)

    b, s, q = bronze.count(), silver.count(), quarantine.count()
    distinct_ids = silver.select("event_id").distinct().count()

    print("\n== Row reconciliation ==")
    print(f"bronze={b} silver={s} quarantine={q}")
    print(f"unaccounted (bronze - silver - quarantine) = {b - s - q}")
    print("  = duplicates removed + rows dropped as late + rows still in flight.")
    print("  Stop the producer and wait ~3 minutes before running for the exact figure.")
    print(f"silver duplicate event_ids = {s - distinct_ids}")

    print("\n== Quarantine by reason ==")
    quarantine.groupBy("reason").count().orderBy(F.desc("count")).show(truncate=False)

    print(f"== Injected test events (wiki = {TEST_WIKI}) in silver ==")
    silver.where(F.col("wiki") == TEST_WIKI).groupBy("event_id").count().show(truncate=False)

    print("== Latest silver rows ==")
    (silver.orderBy(F.desc("event_time"))
     .select("event_time", "wiki", "event_type", "user_name", "is_bot", "title", "byte_delta")
     .show(5, truncate=40))

    for label, path in [("silver", config.SILVER_PATH),
                        ("quarantine", config.QUARANTINE_PATH)]:
        files, mb = delta_detail(spark, path)
        print(f"{label}: files={files} size_mb={mb:.1f}")

    lat = (
        silver.where(F.col("processed_at") > F.current_timestamp() - F.expr("INTERVAL 5 MINUTES"))
        .select((F.col("processed_at").cast("double")
                 - F.col("event_time").cast("double")).alias("lat"))
        .agg(F.count("*").alias("n"),
             F.percentile_approx("lat", 0.5).alias("p50"),
             F.percentile_approx("lat", 0.95).alias("p95"))
        .first()
    )
    print("\n== Event time -> silver latency (rows processed in last 5 min) ==")
    if lat["n"]:
        print(f"n={lat['n']} p50={lat['p50']:.1f}s p95={lat['p95']:.1f}s")
    else:
        print("No rows processed in the last 5 minutes (is the producer running?)")
    spark.stop()


if __name__ == "__main__":
    main()