"""Streaming validation for exact monthly extracts of the UCI source file."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime

from power_pipeline.models import QualityMetrics

EXPECTED_HEADER = (
    "Date;Time;Global_active_power;Global_reactive_power;Voltage;Global_intensity;"
    "Sub_metering_1;Sub_metering_2;Sub_metering_3"
)
EXPECTED_COLUMNS = 9
MEASUREMENT_SLICE = slice(2, 9)


class DataQualityError(ValueError):
    """Raised when a batch must not be published."""


def parse_uci_timestamp(date_value: str, time_value: str) -> datetime:
    return datetime.strptime(f"{date_value} {time_value}", "%d/%m/%Y %H:%M:%S")


def validate_batch_lines(lines: Iterable[str], *, year: int, month: int) -> QualityMetrics:
    timestamps: set[datetime] = set()
    duplicates = 0
    invalid = 0
    missing_measurements = 0
    first: datetime | None = None
    last: datetime | None = None
    row_count = 0

    for raw_line in lines:
        line = raw_line.rstrip("\r\n")
        if not line:
            continue
        fields = line.split(";")
        if len(fields) != EXPECTED_COLUMNS:
            invalid += 1
            continue
        try:
            timestamp = parse_uci_timestamp(fields[0], fields[1])
        except ValueError:
            invalid += 1
            continue
        if timestamp.year != year or timestamp.month != month:
            invalid += 1
            continue
        if timestamp in timestamps:
            duplicates += 1
        timestamps.add(timestamp)
        if any(value in {"", "?"} for value in fields[MEASUREMENT_SLICE]):
            missing_measurements += 1
        first = timestamp if first is None or timestamp < first else first
        last = timestamp if last is None or timestamp > last else last
        row_count += 1

    if invalid:
        raise DataQualityError(f"Batch contains {invalid} malformed or out-of-period rows")
    if row_count == 0:
        raise DataQualityError(f"No rows found for {year:04d}-{month:02d}")
    if duplicates:
        raise DataQualityError(f"Batch contains {duplicates} duplicate timestamps")
    assert first is not None and last is not None
    return QualityMetrics(
        row_count=row_count,
        duplicate_timestamps=duplicates,
        invalid_rows=invalid,
        missing_measurement_rows=missing_measurements,
        first_timestamp=first.isoformat(),
        last_timestamp=last.isoformat(),
    )
