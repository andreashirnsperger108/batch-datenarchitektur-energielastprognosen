from __future__ import annotations

import pytest

from power_pipeline.quality import DataQualityError, validate_batch_lines


def test_valid_batch_reports_missing_measurements() -> None:
    lines = [
        "16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000\n",
        "16/12/2006;17:25:00;?;?;233.630;23.000;0.000;1.000;16.000\n",
    ]

    result = validate_batch_lines(lines, year=2006, month=12)

    assert result.row_count == 2
    assert result.missing_measurement_rows == 1
    assert result.duplicate_timestamps == 0
    assert result.first_timestamp == "2006-12-16T17:24:00"


def test_duplicate_timestamp_fails_gate() -> None:
    line = "16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0;1;17\n"

    with pytest.raises(DataQualityError, match="duplicate"):
        validate_batch_lines([line, line], year=2006, month=12)


def test_out_of_period_row_fails_gate() -> None:
    line = "01/01/2007;00:00:00;1.0;0.1;230;4;0;0;0\n"

    with pytest.raises(DataQualityError, match="out-of-period"):
        validate_batch_lines([line], year=2006, month=12)

