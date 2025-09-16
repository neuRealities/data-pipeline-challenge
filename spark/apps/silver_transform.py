#!/usr/bin/env python3
# ------------------------------------------------------------
# Silver Transform — Spark Batch (Bronze → Silver)
#
# Purpose:
#   - Read Bronze Delta events for a target date.
#   - Normalize event-specific payloads.
#   - Aggregate user progress metrics.
#   - Overwrite the target partition in Silver (dynamic).
#
# Operational notes:
#   - Spark master is set via spark_config (spark://spark-master:7077).
#   - Accepts --run_date (YYYY-MM-DD) or defaults to yesterday (UTC).
#   - S3A/Delta configured via spark-defaults.conf.
#   - Partition overwrite mode configured here (dynamic).
#   - Output partitioned by event_date. Lineage columns included.
# ------------------------------------------------------------

import argparse
from datetime import datetime, timedelta, timezone
from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date,
    when, sum as F_sum, avg as F_avg, countDistinct
)
from spark_config import build_spark, s3a

BRONZE_PATH = s3a("bronze", "lms_events")
SILVER_PATH = s3a("silver", "user_progress")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--run_date", type=str, default=None, help="YYYY-MM-DD (UTC). Defaults to yesterday.")
    return p.parse_args()


def default_yesterday_utc():
    return (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()


def main():
    args = parse_args()
    run_date = args.run_date or default_yesterday_utc()

    # Use extra_conf hook from spark_config for job-specific settings
    spark = build_spark(
        "silver-transform",
        extra_conf={"spark.sql.sources.partitionOverwriteMode": "dynamic"},
    )

    print(f"[silver-transform] Spark master = {spark.sparkContext.master}", flush=True)
    print(f"[silver-transform] run_date = {run_date}", flush=True)

    bronze = (
        spark.read.format("delta").load(BRONZE_PATH)
        .where(col("event_date") == lit(run_date))
    )

    # Normalize and aggregate per user_id, event_date
    df = bronze.select(
        col("user_id"),
        col("course_id"),
        col("event_date"),
        col("event_type"),
        col("payload.*"),
    )

    user_course = df.groupBy("user_id", "event_date").agg(
        F_sum(when(col("event_type") == "course_enrolled", 1).otherwise(0)).alias("courses_enrolled"),
        F_sum(when(col("event_type") == "video_started", 1).otherwise(0)).alias("videos_started"),
        F_sum(when(col("event_type") == "video_completed", 1).otherwise(0)).alias("videos_completed"),
        F_sum(when(col("event_type") == "quiz_attempted", 1).otherwise(0)).alias("quizzes_attempted"),
        F_sum(when(col("event_type") == "quiz_submitted", 1).otherwise(0)).alias("quizzes_submitted"),
        F_sum(when(col("event_type") == "article_viewed", 1).otherwise(0)).alias("articles_viewed"),
        countDistinct("course_id").alias("unique_courses_accessed"),
        F_sum(lit(1)).alias("total_activities"),
        F_avg(when(col("event_type") == "quiz_submitted", col("score"))).alias("avg_quiz_score"),
        F_sum(when(col("event_type") == "video_completed", col("watch_seconds")).otherwise(0)).alias("total_watch_seconds"),
    )

    enriched = (
        user_course
        .withColumn(
            "video_completion_rate_percent",
            (col("videos_completed") / (col("videos_started") + col("videos_completed")).cast("double")) * 100.0
        )
        .withColumn(
            "quiz_completion_rate_percent",
            (col("quizzes_submitted") / (col("quizzes_attempted") + col("quizzes_submitted")).cast("double")) * 100.0
        )
        .withColumn(
            "engagement_level",
            when(col("total_activities") >= 5, "High")
            .when(col("total_activities") >= 2, "Medium")
            .otherwise("Low")
        )
        .withColumn("is_active_learner", col("total_activities") >= 1)
        .withColumn(
            "completion_score",
            ((col("video_completion_rate_percent") + col("quiz_completion_rate_percent")) / 2.0)
        )
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("processing_date", current_date())
    )

    
    def delta_exists(path: str) -> bool:
        try:
            # JVM Delta API: avoids needing the Python delta-spark package
            spark._jvm.io.delta.tables.DeltaTable.forPath(spark._jsparkSession, path)
            return True
        except Exception:
            return False

    writer = (
        enriched.repartition(1)  # single file per partition at current volumes
        .write
        .format("delta")
        .partitionBy("event_date")
    )

    current_mode = spark.conf.get("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if not delta_exists(SILVER_PATH):
        # First create: must be static if using overwriteSchema
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        try:
            (writer
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(SILVER_PATH))
        finally:
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", current_mode)
    else:
        # Subsequent runs: keep dynamic and scope overwrite to the run_date
        (writer
            .mode("overwrite")
            .option("replaceWhere", f"event_date = date'{run_date}'")
            .save(SILVER_PATH))


if __name__ == "__main__":
    main()

