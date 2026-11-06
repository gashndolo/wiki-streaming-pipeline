from pyspark.sql.types import (BooleanType, IntegerType, LongType, StringType,
                               StructField, StructType)

_OLD_NEW = StructType([
    StructField("old", LongType()),
    StructField("new", LongType()),
])

EVENT_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("type", StringType()),
    StructField("namespace", IntegerType()),
    StructField("title", StringType()),
    StructField("timestamp", LongType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField("patrolled", BooleanType()),
    StructField("length", _OLD_NEW),
    StructField("revision", _OLD_NEW),
    StructField("server_name", StringType()),
    StructField("wiki", StringType()),
    StructField("meta", StructType([
        StructField("id", StringType()),
        StructField("domain", StringType()),
        StructField("dt", StringType()),
    ])),
])