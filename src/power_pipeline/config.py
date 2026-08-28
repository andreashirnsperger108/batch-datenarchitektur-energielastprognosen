"""Environment-based configuration with safe defaults for the local project."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    source_url: str
    expected_source_sha256: str | None
    cache_dir: Path
    webhdfs_url: str
    hdfs_user: str
    log_level: str
    request_timeout_seconds: int

    @classmethod
    def from_env(cls) -> Settings:
        expected = os.getenv("EXPECTED_SOURCE_SHA256", "").strip().lower() or None
        return cls(
            source_url=os.getenv(
                "UCI_SOURCE_URL",
                "https://archive.ics.uci.edu/static/public/235/"
                "individual+household+electric+power+consumption.zip",
            ),
            expected_source_sha256=expected,
            cache_dir=Path(os.getenv("SOURCE_CACHE_DIR", "/var/cache/power-pipeline")),
            webhdfs_url=os.getenv("WEBHDFS_URL", "http://namenode:9870").rstrip("/"),
            hdfs_user=os.getenv("HDFS_INGESTION_USER", "ingestion"),
            log_level=os.getenv("PIPELINE_LOG_LEVEL", "INFO").upper(),
            request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "120")),
        )
