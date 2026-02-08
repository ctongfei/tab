"""Local filesystem storage backend."""

import os
from glob import glob
from typing import BinaryIO, Iterator

from tab_cli.storage.base import FileInfo, StorageBackend


class LocalBackend(StorageBackend):
    """Storage backend for local filesystem."""

    def open(self, url: str) -> BinaryIO:
        return open(url, "rb")

    def list_files(self, url: str, extension: str) -> Iterator[FileInfo]:
        pattern = os.path.join(url, "**", f"*{extension}")
        for path in sorted(glob(pattern, recursive=True)):
            yield FileInfo(url=path, size=os.path.getsize(path))

    def size(self, url: str) -> int:
        return os.path.getsize(url)

    def is_directory(self, url: str) -> bool:
        return os.path.isdir(url)
