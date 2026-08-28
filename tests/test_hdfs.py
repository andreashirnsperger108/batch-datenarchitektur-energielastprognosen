from __future__ import annotations

import pytest

from power_pipeline.hdfs import WebHdfsClient


@pytest.mark.parametrize("path", ["relative/path", "/data/../secret"])
def test_rejects_unsafe_hdfs_paths(path: str) -> None:
    client = WebHdfsClient("http://namenode:9870", "ingestion")

    with pytest.raises(ValueError, match="Unsafe HDFS path"):
        client._url(path)


def test_encodes_hdfs_path_without_losing_partitions() -> None:
    client = WebHdfsClient("http://namenode:9870", "ingestion")

    url = client._url("/data/raw/source=uci/year=2006/month=12")

    assert url.endswith("/data/raw/source%3Duci/year%3D2006/month%3D12")

