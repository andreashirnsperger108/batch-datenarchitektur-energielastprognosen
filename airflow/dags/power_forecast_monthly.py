"""Monthly historical batch DAG for the UCI energy-consumption pipeline."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import requests
from airflow.exceptions import AirflowException
from airflow.sdk import dag, get_current_context, task

INGESTION_API_URL = os.getenv("INGESTION_API_URL", "http://ingestion-api:8000").rstrip("/")
SPARK_DRIVER_API_URL = os.getenv(
    "SPARK_DRIVER_API_URL", "http://spark-driver-api:8001"
).rstrip("/")


def _target_period() -> tuple[int, int]:
    context = get_current_context()
    dag_run = context["dag_run"]
    configuration = dag_run.conf or {}
    if "year" in configuration and "month" in configuration:
        return int(configuration["year"]), int(configuration["month"])
    interval = context["data_interval_start"]
    return interval.year, interval.month


@dag(
    dag_id="power_forecast_monthly",
    description="UCI monthly ingestion, quality gates, Spark aggregation and quarterly snapshot",
    schedule="@monthly",
    start_date=datetime(2006, 12, 1, tzinfo=UTC),
    end_date=datetime(2010, 12, 1, tzinfo=UTC),
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
    },
    tags=["batch", "energy", "uci"],
)
def power_forecast_monthly():
    @task
    def source_check() -> dict:
        year, month = _target_period()
        if not 2006 <= year <= 2010 or not 1 <= month <= 12:
            raise AirflowException(f"Unsupported UCI period: {year:04d}-{month:02d}")
        response = requests.get(f"{INGESTION_API_URL}/health", timeout=10)
        response.raise_for_status()
        return {"year": year, "month": month, "source_service": response.json()}

    @task
    def ingest_month(period: dict) -> dict:
        response = requests.post(
            f"{INGESTION_API_URL}/batches/{period['year']}/{period['month']}",
            timeout=900,
        )
        response.raise_for_status()
        return response.json()

    @task
    def raw_quality_gate(ingestion_result: dict) -> dict:
        manifest = ingestion_result["manifest"]
        quality = manifest["quality"]
        if quality["row_count"] <= 0:
            raise AirflowException("Raw quality gate rejected an empty batch")
        if quality["invalid_rows"] or quality["duplicate_timestamps"]:
            raise AirflowException(f"Raw quality gate failed: {quality}")
        return manifest

    @task(execution_timeout=timedelta(hours=1))
    def spark_transform(manifest: dict) -> dict:
        response = requests.post(
            f"{SPARK_DRIVER_API_URL}/transform/{manifest['year']}/{manifest['month']}/"
            f"{manifest['batch_id']}",
            timeout=3660,
        )
        if not response.ok:
            raise AirflowException(f"Spark transformation failed: {response.text[-4000:]}")
        return response.json()

    @task
    def record_lineage(spark_result: dict, manifest: dict) -> dict:
        return {
            "status": "success",
            "batch_id": manifest["batch_id"],
            "raw_hdfs_path": manifest["raw_hdfs_path"],
            "git_commit": os.getenv("GIT_COMMIT", "unknown"),
            "spark_status": spark_result["status"],
        }

    period = source_check()
    ingestion = ingest_month(period)
    manifest = raw_quality_gate(ingestion)
    transformed = spark_transform(manifest)
    record_lineage(transformed, manifest)


power_forecast_monthly()
