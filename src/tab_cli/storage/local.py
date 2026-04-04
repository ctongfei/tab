"""Local filesystem storage backend."""

import os
from glob import glob
from typing import BinaryIO, Iterator

from tab_cli.storage.base import FileInfo, StorageBackend, has_glob_pattern


class LocalBackend(StorageBackend):
    """Storage backend for local filesystem."""

    def open(self, url: str) -> BinaryIO:
        return open(url, "rb")

    def list_files(self, url: str, extension: str) -> Iterator[FileInfo]:
        is_glob = has_glob_pattern(url)
        if is_glob:
            pattern = url
        elif extension:
            pattern = os.path.join(url, "**", f"*.{extension}")
        else:
            pattern = os.path.join(url, "**", "*")
        for path in sorted(glob(pattern, recursive=True)):
            if os.path.isfile(path) is False:
                continue
            if not is_glob and extension and not path.endswith(f".{extension}"):
                continue
            yield FileInfo(url=path, size=os.path.getsize(path))

    def size(self, url: str) -> int:
        return os.path.getsize(url)

    def is_directory(self, url: str) -> bool:
        return os.path.isdir(url)
