"""Download, monthly extraction, validation, manifesting and atomic HDFS publication."""

from __future__ import annotations

import hashlib
import logging
import os
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import requests

from power_pipeline.config import Settings
from power_pipeline.hdfs import WebHdfsClient
from power_pipeline.models import BatchManifest
from power_pipeline.quality import EXPECTED_HEADER, DataQualityError, validate_batch_lines

LOGGER = logging.getLogger(__name__)
SOURCE_ARCHIVE_NAME = "individual-household-electric-power-consumption.zip"
SOURCE_TEXT_NAME = "household_power_consumption.txt"


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_source(settings: Settings) -> tuple[Path, str]:
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    archive_path = settings.cache_dir / SOURCE_ARCHIVE_NAME
    extracted_path = settings.cache_dir / SOURCE_TEXT_NAME

    if not archive_path.exists():
        partial = archive_path.with_suffix(".part")
        LOGGER.info("Downloading UCI source", extra={"event": "source_download"})
        with requests.get(
            settings.source_url,
            stream=True,
            timeout=settings.request_timeout_seconds,
        ) as response:
            response.raise_for_status()
            with partial.open("wb") as target:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        target.write(chunk)
        partial.replace(archive_path)

    source_sha256 = sha256_file(archive_path)
    if settings.expected_source_sha256 and source_sha256 != settings.expected_source_sha256:
        raise DataQualityError(
            "Downloaded source checksum does not match EXPECTED_SOURCE_SHA256"
        )

    if not extracted_path.exists():
        with zipfile.ZipFile(archive_path) as archive:
            members = {Path(name).name: name for name in archive.namelist()}
            member = members.get(SOURCE_TEXT_NAME)
            if not member:
                raise DataQualityError(f"{SOURCE_TEXT_NAME} is missing from the source archive")
            temporary = extracted_path.with_suffix(".part")
            with archive.open(member) as source, temporary.open("wb") as target:
                while chunk := source.read(1024 * 1024):
                    target.write(chunk)
            temporary.replace(extracted_path)
    return extracted_path, source_sha256


def extract_month(source_path: Path, target_path: Path, *, year: int, month: int) -> None:
    with source_path.open("r", encoding="utf-8", newline="") as source:
        header = source.readline().strip()
        if header != EXPECTED_HEADER:
            raise DataQualityError(f"Unexpected UCI schema header: {header!r}")
        with target_path.open("w", encoding="utf-8", newline="\n") as target:
            target.write(header + "\n")
            for line in source:
                fields = line.split(";", 2)
                if len(fields) < 2:
                    continue
                date_parts = fields[0].split("/")
                if len(date_parts) != 3:
                    continue
                if int(date_parts[2]) == year and int(date_parts[1]) == month:
                    target.write(line.rstrip("\r\n") + "\n")


def ingest_month(year: int, month: int, settings: Settings) -> dict:
    if not 2006 <= year <= 2010 or not 1 <= month <= 12:
        raise ValueError("The UCI source covers calendar months between 2006 and 2010")

    source_path, source_sha256 = ensure_source(settings)
    hdfs = WebHdfsClient(
        settings.webhdfs_url,
        user=settings.hdfs_user,
        timeout=settings.request_timeout_seconds,
    )

    with tempfile.TemporaryDirectory(prefix="uci-month-") as temp_dir:
        batch_file = Path(temp_dir) / f"household_power_{year:04d}_{month:02d}.csv"
        extract_month(source_path, batch_file, year=year, month=month)
        with batch_file.open("r", encoding="utf-8") as stream:
            header = stream.readline().strip()
            if header != EXPECTED_HEADER:
                raise DataQualityError("Extracted batch header does not match the source schema")
            quality = validate_batch_lines(stream, year=year, month=month)

        batch_sha256 = sha256_file(batch_file)
        batch_id = f"uci-{year:04d}-{month:02d}-{batch_sha256[:12]}"
        final_dir = f"/data/raw/source=uci/year={year:04d}/month={month:02d}/batch_id={batch_id}"
        final_manifest = f"{final_dir}/manifest.json"
        if hdfs.exists(final_manifest):
            existing = hdfs.read_json(final_manifest)
            if existing.get("batch_sha256") != batch_sha256:
                raise DataQualityError("Existing batch_id has a different checksum")
            LOGGER.info(
                "Batch already published; idempotent no-op",
                extra={"batch_id": batch_id, "year": year, "month": month},
            )
            return {"status": "already_exists", "manifest": existing}

        stage_dir = f"/data/_staging/ingestion/{batch_id}"
        hdfs.mkdirs(stage_dir, permission="700")
        manifest = BatchManifest(
            schema_version="1.0",
            batch_id=batch_id,
            source_name="Individual Household Electric Power Consumption",
            source_url=settings.source_url,
            source_license="CC BY 4.0",
            source_doi="10.24432/C58K54",
            source_sha256=source_sha256,
            batch_sha256=batch_sha256,
            year=year,
            month=month,
            created_at_utc=datetime.now(UTC).isoformat(),
            raw_hdfs_path=final_dir,
            code_commit=os.getenv("GIT_COMMIT", "unknown"),
            quality=quality,
        )
        hdfs.upload_file(batch_file, f"{stage_dir}/data.csv")
        hdfs.upload_json(manifest.as_dict(), f"{stage_dir}/manifest.json")
        hdfs.mkdirs(str(Path(final_dir).parent).replace("\\", "/"))
        hdfs.rename(stage_dir, final_dir)
        hdfs.set_permission(final_dir, "755")
        LOGGER.info(
            "Batch published",
            extra={"batch_id": batch_id, "year": year, "month": month},
        )
        return {"status": "published", "manifest": manifest.as_dict()}


def write_quarantine_report(
    *, settings: Settings, year: int, month: int, error: Exception
) -> None:
    hdfs = WebHdfsClient(settings.webhdfs_url, settings.hdfs_user)
    report_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    try:
        hdfs.mkdirs(f"/data/quarantine/year={year:04d}/month={month:02d}")
        hdfs.upload_json(
            {
                "year": year,
                "month": month,
                "failed_at_utc": datetime.now(UTC).isoformat(),
                "error_type": type(error).__name__,
                "message": str(error),
            },
            f"/data/quarantine/year={year:04d}/month={month:02d}/{report_id}.json",
        )
    except Exception:
        LOGGER.exception("Could not persist quarantine report")
