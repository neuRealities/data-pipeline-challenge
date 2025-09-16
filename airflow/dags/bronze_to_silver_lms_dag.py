# ------------------------------------------------------------
# Bronze → Silver DAG (Airflow)
#
# Purpose:
#   - Run the Spark batch job that transforms Bronze events into Silver.
#   - Execute once per day.
#
# ------------------------------------------------------------
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEFAULT_ARGS = {
    "owner": "data-pipeline-challenge",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

MINIO_ENV = {"MC_HOST_minio": "http://minioadmin:minioadmin@minio:9000"}

with DAG(
    dag_id="bronze_to_silver_lms",
    description="Read Bronze (Delta on MinIO) and write Silver user_progress (Delta).",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="15 2 * * *",  # daily at 02:15
    catchup=True,                     # allow backfills for past dates
    max_active_runs=1,
    tags=["lms", "delta", "spark", "silver"],
) as dag:

    ensure_buckets = BashOperator(
        task_id="ensure_buckets",
        bash_command="""
          set -euo pipefail
          export HOME=/home/airflow
          export MC_CONFIG_DIR=/home/airflow/.mc
          echo "[ensure] Ensuring datalake buckets exist..."
          mc --config-dir "$MC_CONFIG_DIR" mb -p minio/datalake/bronze      || true
          mc --config-dir "$MC_CONFIG_DIR" mb -p minio/datalake/silver      || true
          mc --config-dir "$MC_CONFIG_DIR" mb -p minio/datalake/checkpoints || true
          mc --config-dir "$MC_CONFIG_DIR" mb -p minio/datalake/analytics   || true
          echo "[ensure] Buckets ready."
        """,
        env=MINIO_ENV,
        do_xcom_push=False,
    )

    precheck_bronze_exists = BashOperator(
        task_id="precheck_bronze_exists",
        bash_command="""
          set -euo pipefail
          export HOME=/home/airflow
          export MC_CONFIG_DIR=/home/airflow/.mc
          echo "[precheck] Checking Bronze path: minio/datalake/bronze/lms_events"
          mc --config-dir "$MC_CONFIG_DIR" ls minio/datalake/bronze/lms_events | sed -e 's/^/  /'
          echo "[precheck] Bronze path is reachable."
        """,
        env=MINIO_ENV,
        do_xcom_push=False,
    )

    silver_transform = SparkSubmitOperator(
        task_id="silver_transform",
        conn_id="spark_default",
        application="/opt/spark/apps/silver_transform.py",
        application_args=["--run_date", "{{ ds }}"],       # Pass Airflow logical date (YYYY-MM-DD)
        driver_memory="1g",
        executor_memory="2g",
        executor_cores=1,
        num_executors=1,
        verbose=False,
    )

    postcheck_silver_exists = BashOperator(
        task_id="postcheck_silver_exists",
        bash_command="""
          set -euo pipefail
          export HOME=/home/airflow
          export MC_CONFIG_DIR=/home/airflow/.mc
          echo "[postcheck] Checking Silver path: minio/datalake/silver/user_progress for {{ ds }}"
          mc --config-dir "$MC_CONFIG_DIR" ls minio/datalake/silver/user_progress/event_date={{ ds }} | sed -e 's/^/  /'
          echo "[postcheck] Silver path is present for {{ ds }}."
        """,
        env=MINIO_ENV,
        trigger_rule="all_success",
        do_xcom_push=False,
    )

    ensure_buckets >> precheck_bronze_exists >> silver_transform >> postcheck_silver_exists
