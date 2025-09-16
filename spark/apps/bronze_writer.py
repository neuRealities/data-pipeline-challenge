#!/usr/bin/env python3
# ------------------------------------------------------------
# Bronze Writer — Spark Structured Streaming (Kafka → Delta/MinIO)
#
# - Reads Kafka topic `lms_events`.
# - Parses JSON into typed columns per explicit schema.
# - Appends to Delta at s3a://<S3_BUCKET>/bronze/lms_events.
# - Uses a stable checkpoint for exactly-once semantics.
# - Prints key runtime info and surfaces stream errors (if any).
# ------------------------------------------------------------

import os
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp, to_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType
)
from spark_config import build_spark, s3a

# ---------------------------
# Config (env-overridable)
# ---------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "lms_events")

BRONZE_PATH = s3a("bronze", "lms_events")
CHECKPOINT_PATH = s3a("_checkpoints", "bronze", "lms_events")

# ---------------------------
# Schemas (explicit)
# ---------------------------
payload_schema = StructType([
    StructField("enroll_method", StringType(), True),
    StructField("cohort", StringType(), True),

    StructField("video_id", StringType(), True),
    StructField("position_seconds", LongType(), True),
    StructField("watch_seconds", LongType(), True),

    StructField("quiz_id", StringType(), True),
    StructField("questions", LongType(), True),
    StructField("score", DoubleType(), True),
    StructField("passed", BooleanType(), True),

    StructField("article_id", StringType(), True),
    StructField("read_seconds", LongType(), True),
])

event_schema = StructType([
    StructField("schema_version", StringType(), True),
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("course_id", StringType(), True),
    StructField("timestamp_iso", StringType(), False),
    StructField("timestamp_ms", LongType(), False),
    StructField("payload", payload_schema, True),
])

# ---------------------------
# Main
# ---------------------------
def main() -> None:
    # Build Spark with cluster master + S3A creds (Delta/Extensions via spark-defaults.conf)
    spark = build_spark("bronze-writer")

    # Reduce noisy INFO logs during dev
    spark.sparkContext.setLogLevel("WARN")

    print(f"[bronze-writer] Spark master = {spark.sparkContext.master}", flush=True)
    print(f"[bronze-writer] bootstrap={KAFKA_BOOTSTRAP} topic={KAFKA_TOPIC}", flush=True)
    print(f"[bronze-writer] bronze={BRONZE_PATH}", flush=True)
    print(f"[bronze-writer] checkpoint={CHECKPOINT_PATH}", flush=True)

    # Kafka source (binary key/value)
    raw = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
             .option("subscribe", KAFKA_TOPIC)
             .option("startingOffsets", "earliest")
             # helpful in dev; prevents immediate failure if offsets are missing
             .option("failOnDataLoss", "false")
             .option("maxOffsetsPerTrigger", 1000) \
             .load()
    )

    # Parse to typed columns
    parsed = (
        raw.selectExpr("CAST(key AS STRING) AS k", "CAST(value AS STRING) AS v", "partition", "offset")
           .withColumn("json", from_json(col("v"), event_schema))
           .select("json.*", "partition", "offset")
           .withColumn("event_time", to_timestamp(col("timestamp_iso")))
           .withColumn("ingest_time", current_timestamp())
           .withColumn("event_date", to_date(col("event_time")))
    )

    # Start streaming write to Delta (append)
    query = (
        parsed.writeStream
              .format("delta")
              .outputMode("append")
              .option("checkpointLocation", CHECKPOINT_PATH)
              .option("mergeSchema", "true") \
              .option("maxFilesPerTrigger", 10) \
              .partitionBy("event_date") \
              .trigger(processingTime="30 seconds")
              .start(BRONZE_PATH)
    )

    print("[bronze-writer] query started?", query.isActive, flush=True)

    try:
        query.awaitTermination()
    except Exception as e:
        import traceback
        print("[bronze-writer] stream error:", e, flush=True)
        traceback.print_exc()
        # re-raise so spark-submit exits nonzero (easier to catch in Airflow/manual runs)
        raise


if __name__ == "__main__":
    main()
