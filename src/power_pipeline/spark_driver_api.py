"""Internal API that runs spark-submit in client mode without exposing Docker's socket."""

from __future__ import annotations

import os
import subprocess
import tempfile

from fastapi import FastAPI, HTTPException

app = FastAPI(
    title="Spark Driver API",
    version="0.1.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "healthy"}


@app.post("/transform/{year}/{month}/{batch_id}")
def transform(year: int, month: int, batch_id: str) -> dict:
    if not 2006 <= year <= 2010 or not 1 <= month <= 12:
        raise HTTPException(status_code=422, detail="Invalid UCI period")
    expected_prefix = f"uci-{year:04d}-{month:02d}-"
    suffix = batch_id.removeprefix(expected_prefix)
    if not batch_id.startswith(expected_prefix) or len(suffix) != 12 or not suffix.isalnum():
        raise HTTPException(status_code=422, detail="Invalid deterministic batch_id")

    command = [
        "/opt/spark/bin/spark-submit",
        "--master",
        os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
        "--deploy-mode",
        "client",
        "--name",
        f"power-transform-{year:04d}-{month:02d}",
        "--conf",
        "spark.cores.max=2",
        "--conf",
        "spark.executor.cores=1",
        "--conf",
        "spark.executor.memory=1g",
        "--conf",
        "spark.driver.memory=1g",
        "--conf",
        "spark.driver.host=spark-driver-api",
        "--conf",
        "spark.driver.bindAddress=0.0.0.0",
        "/opt/pipeline/spark_transform.py",
        "--year",
        str(year),
        "--month",
        str(month),
        "--batch-id",
        batch_id,
        "--hdfs-uri",
        os.getenv("HDFS_URI", "hdfs://namenode:9000"),
        "--max-null-ratio",
        os.getenv("MAX_NULL_RATIO", "0.05"),
    ]
    with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as log_file:
        try:
            subprocess.run(  # noqa: S603 - every argument is validated/fixed above
                command,
                check=True,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=3600,
            )
        except subprocess.TimeoutExpired as error:
            raise HTTPException(status_code=504, detail="Spark job exceeded one hour") from error
        except subprocess.CalledProcessError as error:
            log_file.seek(0)
            detail = (log_file.read() or "Spark job failed")[-4000:]
            raise HTTPException(status_code=500, detail=detail) from error
        log_file.seek(0)
        log_tail = log_file.read()[-4000:]
    return {
        "status": "completed",
        "batch_id": batch_id,
        "log_tail": log_tail,
    }
