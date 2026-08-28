"""Internal HTTP boundary for the Python ingestion microservice."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool

from power_pipeline.config import Settings
from power_pipeline.ingestion import ingest_month, write_quarantine_report
from power_pipeline.logging_utils import configure_logging

settings = Settings.from_env()
configure_logging(settings.log_level)
LOGGER = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    yield


app = FastAPI(
    title="Power Pipeline Ingestion API",
    version="0.1.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "healthy"}


@app.post("/batches/{year}/{month}")
async def create_batch(year: int, month: int) -> dict:
    try:
        return await run_in_threadpool(ingest_month, year, month, settings)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except Exception as error:
        LOGGER.exception("Ingestion failed", extra={"year": year, "month": month})
        await run_in_threadpool(
            write_quarantine_report,
            settings=settings,
            year=year,
            month=month,
            error=error,
        )
        raise HTTPException(
            status_code=500,
            detail="Ingestion failed; see structured logs",
        ) from error
