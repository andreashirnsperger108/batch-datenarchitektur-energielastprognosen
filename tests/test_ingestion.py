from __future__ import annotations

from pathlib import Path

from power_pipeline.ingestion import extract_month, sha256_file
from power_pipeline.quality import EXPECTED_HEADER


def test_extract_month_preserves_source_rows(tmp_path: Path) -> None:
    source = tmp_path / "source.txt"
    december = "16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0;1;17"
    january = "01/01/2007;00:00:00;1.000;0.100;230.000;4.000;0;0;0"
    source.write_text(
        f"{EXPECTED_HEADER}\n{december}\n{january}\n",
        encoding="utf-8",
    )
    target = tmp_path / "month.csv"

    extract_month(source, target, year=2006, month=12)

    assert target.read_text(encoding="utf-8") == f"{EXPECTED_HEADER}\n{december}\n"


def test_sha256_file_is_deterministic(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.write_bytes(b"same batch")
    second.write_bytes(b"same batch")

    assert sha256_file(first) == sha256_file(second)

