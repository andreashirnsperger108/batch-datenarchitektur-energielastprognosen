"""Serializable batch metadata models."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class QualityMetrics:
    row_count: int
    duplicate_timestamps: int
    invalid_rows: int
    missing_measurement_rows: int
    first_timestamp: str
    last_timestamp: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BatchManifest:
    schema_version: str
    batch_id: str
    source_name: str
    source_url: str
    source_license: str
    source_doi: str
    source_sha256: str
    batch_sha256: str
    year: int
    month: int
    created_at_utc: str
    raw_hdfs_path: str
    code_commit: str
    quality: QualityMetrics

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["quality"] = self.quality.as_dict()
        return data

