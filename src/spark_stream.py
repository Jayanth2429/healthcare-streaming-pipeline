from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import MapType, StringType, StructField, StructType

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "healthcare-events")
CURATED_PATH = os.getenv("CURATED_PATH", "data/curated/events")
QUARANTINE_PATH = os.getenv("QUARANTINE_PATH", "data/quarantine/events")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "data/checkpoints")

ALLOWED_EVENT_TYPES = ("ADMISSION", "DISCHARGE", "TRANSFER")

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("subject_id", StringType(), True),
        StructField("hadm_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("source_record_id", StringType(), True),
        StructField("care_unit", StringType(), True),
        StructField("payload", MapType(StringType(), StringType()), True),
    ]
)


def is_valid_event(df: DataFrame):
    return (
        col("event_id").isNotNull()
        & col("subject_id").isNotNull()
        & col("source_table").isNotNull()
        & col("source_record_id").isNotNull()
        & col("event_type").isin(*ALLOWED_EVENT_TYPES)
        & col("event_ts").isNotNull()
    )


def build_stream(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    return (
        raw.select(
            col("timestamp").alias("kafka_timestamp"),
            col("value").cast("string").alias("raw_json"),
            from_json(col("value").cast("string"), SCHEMA).alias("event"),
        )
        .select("kafka_timestamp", "raw_json", "event.*")
        .withColumn("event_ts", to_timestamp("event_timestamp"))
    )


def main() -> None:
    spark = SparkSession.builder.appName("healthcare-streaming-pipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    parsed = build_stream(spark)
    validation = is_valid_event(parsed)
    valid = parsed.filter(validation)
    invalid = parsed.filter(~validation)

    curated = (
        valid.withWatermark("event_ts", "10 minutes")
        .dropDuplicates(["event_id"])
        .drop("event_timestamp", "raw_json")
    )

    curated_query = (
        curated.writeStream.format("parquet")
        .option("path", CURATED_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/curated")
        .outputMode("append")
        .queryName("curated_healthcare_events")
        .start()
    )

    quarantine_query = (
        invalid.writeStream.format("json")
        .option("path", QUARANTINE_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/quarantine")
        .outputMode("append")
        .queryName("quarantined_healthcare_events")
        .start()
    )

    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in (curated_query, quarantine_query):
            if query.isActive:
                query.stop()


if __name__ == "__main__":
    main()
