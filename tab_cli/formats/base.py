"""Base format handler interface."""

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import BinaryIO

import polars as pl


class FormatHandler(ABC):
    """Handles reading and writing a specific tabular format."""

    @abstractmethod
    def extension(self) -> str:
        """Return the file extension (e.g., '.parquet')."""
        pass

    def supports_glob(self) -> bool:
        """Whether this format supports glob patterns in scan().

        Formats with native Polars support (Parquet, CSV, JSONL) can scan
        directories directly. Others (Avro) need manual file iteration.
        """
        return False

    @abstractmethod
    def scan(self, url: str, storage_options: dict[str, str] | None = None) -> pl.LazyFrame:
        """Scan from a URL (local path or cloud URL).

        Args:
            url: The URL to scan from.
            storage_options: Optional storage options for cloud access.
        """
        pass

    @abstractmethod
    def read_stream(self, stream: BinaryIO) -> pl.DataFrame:
        """Read from a byte stream. Returns eager DataFrame."""
        pass

    @abstractmethod
    def collect_schema(self, url: str, storage_options: dict[str, str] | None = None) -> list[tuple[str, pl.DataType]]:
        """Get schema as list of (name, dtype) tuples."""
        pass

    @abstractmethod
    def count_rows(self, url: str, storage_options: dict[str, str] | None = None) -> int:
        """Count rows in the file."""
        pass

    def extra_summary(self, url: str) -> dict[str, str | int | float] | None:
        """Return format-specific summary metadata, if any."""
        return None

    @abstractmethod
    def write(self, lf: pl.LazyFrame) -> Iterable[bytes]:
        """Write LazyFrame to bytes (for streaming output)."""
        pass

    @abstractmethod
    def write_to_single_file(self, lf: pl.LazyFrame, path: str) -> None:
        """Write LazyFrame to a single file."""
        pass
