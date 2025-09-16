#!/usr/bin/env python3
# ------------------------------------------------------------
# Spark Config Helper (Maximal)
#
# Purpose:
#   - Creates SparkSession with Delta + S3A + Kafka support.
#   - Ensures critical extensions are always set, even if
#     spark-defaults.conf is not mounted.
#
# Usage:
#   from spark_config import build_spark, s3a
#   spark = build_spark("bronze-writer")
#
# Notes:
#   - Versions aligned with Spark 3.5.2, Delta 3.3.0,
#     Hadoop AWS 3.3.4, AWS SDK 1.12.300.
# ------------------------------------------------------------

import os
from typing import Optional, Mapping
from pyspark.sql import SparkSession

# Master (overridable via .env)
MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

# S3/MinIO (env-driven)
S3_ENDPOINT   = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET     = os.getenv("S3_BUCKET", "datalake")

# Resource caps
EXECUTOR_INSTANCES = os.getenv("SPARK_EXECUTOR_INSTANCES", "1")
EXECUTOR_CORES     = os.getenv("SPARK_EXECUTOR_CORES", "1")
EXECUTOR_MEMORY    = os.getenv("SPARK_EXECUTOR_MEMORY", "1g")
DRIVER_MEMORY      = os.getenv("SPARK_DRIVER_MEMORY", "1g")


def base_conf(builder: SparkSession.Builder) -> SparkSession.Builder:
    """Base Spark config with Delta + S3A extensions baked in."""
    return (
        builder
        .master(MASTER_URL)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Discipline
        .config("spark.sql.session.timeZone", "UTC")
        # Resource caps
        .config("spark.executor.instances", EXECUTOR_INSTANCES)
        .config("spark.executor.cores", EXECUTOR_CORES)
        .config("spark.executor.memory", EXECUTOR_MEMORY)
        .config("spark.driver.memory", DRIVER_MEMORY)
    )


def build_spark(app_name: str, extra_conf: Optional[Mapping[str, str]] = None) -> SparkSession:
    """Build SparkSession with optional job-specific overrides."""
    b = base_conf(SparkSession.builder.appName(app_name))
    if extra_conf:
        for k, v in extra_conf.items():
            b = b.config(k, v)
    return b.getOrCreate()


def s3a(*segments: str) -> str:
    """Build an s3a:// path within the configured bucket."""
    path = "/".join(seg.strip("/") for seg in segments)
    return f"s3a://{S3_BUCKET}/{path}"

