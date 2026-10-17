"""Airflow DAG: one task per medallion layer, run daily for the previous day.

`{{ ds }}` is Airflow's logical date, so backfills (`airflow dags backfill`) replay
exactly the right partitions - safe because every job is idempotent.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
}

with DAG(
    dag_id="shopstream_lakehouse_daily",
    start_date=datetime(2026, 10, 1),
    schedule="30 1 * * *",          # 01:30 UTC, after the source drops land
    catchup=True,
    max_active_runs=1,               # layers depend on previous runs' state
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "orders"],
) as dag:
    tasks = {
        job: SparkSubmitOperator(
            task_id=f"{job}_layer",
            application="/opt/shopstream/src/shopstream/main.py",
            py_files="/opt/shopstream/dist/shopstream.zip",
            packages="io.delta:delta-spark_2.13:4.0.0",
            application_args=["--env", "prod", "--job", job, "--run-date", "{{ ds }}",
                              "--config", "/opt/shopstream/conf/pipeline.yaml"],
            conn_id="spark_default",
            conf={"spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                  "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"},
        )
        for job in ("bronze", "silver", "gold")
    }
    tasks["bronze"] >> tasks["silver"] >> tasks["gold"]
