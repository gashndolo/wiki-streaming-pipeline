from pyspark.sql import functions as F

from spark_jobs import config
from spark_jobs.common import get_spark


def main():
    spark = get_spark("verify_bronze", driver_memory="1g")
    df = spark.read.format("delta").load(config.BRONZE_PATH)

    per_partition = (
        df.groupBy("kafka_partition")
        .agg(F.count("*").alias("rows"),
             F.countDistinct("kafka_offset").alias("distinct_offsets"),
             F.min("kafka_offset").alias("min_offset"),
             F.max("kafka_offset").alias("max_offset"))
        .withColumn("duplicates", F.col("rows") - F.col("distinct_offsets"))
        .withColumn("gaps", F.col("max_offset") - F.col("min_offset") + 1
                    - F.col("distinct_offsets"))
        .orderBy("kafka_partition")
    )
    print("\n== Integrity per Kafka partition ==")
    per_partition.show(truncate=False)

    totals = per_partition.agg(F.sum("rows").alias("rows"),
                               F.sum("duplicates").alias("duplicates"),
                               F.sum("gaps").alias("gaps")).first()
    print(f"TOTAL rows={totals['rows']} duplicates={totals['duplicates']} "
          f"gaps={totals['gaps']}")

    detail = spark.sql(f"DESCRIBE DETAIL delta.`{config.BRONZE_PATH}`") \
        .select("numFiles", "sizeInBytes").first()
    print(f"\n== Files == numFiles={detail['numFiles']} "
          f"size_mb={detail['sizeInBytes'] / 1e6:.1f}")

    latency = (
        df.where(F.col("ingested_at") > F.current_timestamp() - F.expr("INTERVAL 5 MINUTES"))
        .select((F.col("ingested_at").cast("double")
                 - F.col("kafka_timestamp").cast("double")).alias("latency_s"))
        .agg(F.count("*").alias("n"),
             F.percentile_approx("latency_s", 0.5).alias("p50"),
             F.percentile_approx("latency_s", 0.95).alias("p95"))
        .first()
    )
    print(f"\n== Kafka -> bronze latency (rows ingested in last 5 min) ==\n"
          f"n={latency['n']} p50={latency['p50']:.1f}s p95={latency['p95']:.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()