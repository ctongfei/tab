"""Base storage backend interface."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import BinaryIO, Iterator, Any


@dataclass
class FileInfo:
    url: str
    size: int


class StorageBackend(ABC):

    @abstractmethod
    def open(self, url: str) -> BinaryIO:
        pass

    @abstractmethod
    def list_files(self, url: str, extension: str) -> Iterator[FileInfo]:
        pass

    @abstractmethod
    def size(self, url: str) -> int:
        pass

    @abstractmethod
    def is_directory(self, url: str) -> bool:
        pass

    def normalize_for_polars(self, url: str) -> str:
        return url

    def storage_options(self, url: str) -> dict[str, Any] | None:
        return None
