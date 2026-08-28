"""Minimal WebHDFS client with explicit user identity and atomic rename support."""

from __future__ import annotations

import json
from pathlib import Path, PurePosixPath
from typing import BinaryIO
from urllib.parse import quote

import requests


class WebHdfsError(RuntimeError):
    """Raised for unexpected WebHDFS responses."""


class WebHdfsClient:
    def __init__(self, base_url: str, user: str, timeout: int = 120) -> None:
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.timeout = timeout

    @staticmethod
    def _validate_path(path: str) -> str:
        parsed = PurePosixPath(path)
        if not path.startswith("/") or ".." in parsed.parts:
            raise ValueError(f"Unsafe HDFS path: {path!r}")
        return str(parsed)

    def _url(self, path: str) -> str:
        safe_path = quote(self._validate_path(path), safe="/")
        return f"{self.base_url}/webhdfs/v1{safe_path}"

    def _check(self, response: requests.Response, expected: set[int]) -> requests.Response:
        if response.status_code not in expected:
            snippet = response.text[:500]
            raise WebHdfsError(
                f"WebHDFS returned {response.status_code} for {response.request.method} "
                f"{response.request.url}: {snippet}"
            )
        return response

    def exists(self, path: str) -> bool:
        response = requests.get(
            self._url(path),
            params={"op": "GETFILESTATUS", "user.name": self.user},
            timeout=self.timeout,
        )
        if response.status_code == 404:
            return False
        self._check(response, {200})
        return True

    def mkdirs(self, path: str, permission: str = "755") -> None:
        response = requests.put(
            self._url(path),
            params={"op": "MKDIRS", "user.name": self.user, "permission": permission},
            timeout=self.timeout,
        )
        self._check(response, {200})

    def set_permission(self, path: str, permission: str) -> None:
        response = requests.put(
            self._url(path),
            params={
                "op": "SETPERMISSION",
                "user.name": self.user,
                "permission": permission,
            },
            timeout=self.timeout,
        )
        self._check(response, {200})

    def upload_file(self, local_path: Path, hdfs_path: str, *, overwrite: bool = False) -> None:
        with local_path.open("rb") as stream:
            self.upload_stream(stream, hdfs_path, overwrite=overwrite)

    def upload_stream(self, stream: BinaryIO, hdfs_path: str, *, overwrite: bool = False) -> None:
        response = requests.put(
            self._url(hdfs_path),
            params={
                "op": "CREATE",
                "user.name": self.user,
                "overwrite": str(overwrite).lower(),
                "permission": "644",
            },
            data=stream,
            allow_redirects=True,
            timeout=self.timeout,
        )
        self._check(response, {201})

    def upload_json(self, payload: dict, hdfs_path: str, *, overwrite: bool = False) -> None:
        data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        response = requests.put(
            self._url(hdfs_path),
            params={
                "op": "CREATE",
                "user.name": self.user,
                "overwrite": str(overwrite).lower(),
                "permission": "644",
            },
            data=data,
            allow_redirects=True,
            timeout=self.timeout,
        )
        self._check(response, {201})

    def rename(self, source: str, destination: str) -> None:
        response = requests.put(
            self._url(source),
            params={
                "op": "RENAME",
                "user.name": self.user,
                "destination": self._validate_path(destination),
            },
            timeout=self.timeout,
        )
        self._check(response, {200})
        if not response.json().get("boolean"):
            raise WebHdfsError(f"HDFS rename failed: {source} -> {destination}")

    def read_json(self, path: str) -> dict:
        response = requests.get(
            self._url(path),
            params={"op": "OPEN", "user.name": self.user},
            allow_redirects=True,
            timeout=self.timeout,
        )
        self._check(response, {200})
        return response.json()
