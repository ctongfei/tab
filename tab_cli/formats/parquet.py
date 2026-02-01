"""Parquet format handler."""

from collections.abc import Iterable
from io import BytesIO
from typing import BinaryIO

import polars as pl

from tab_cli.formats.base import FormatHandler


class ParquetFormat(FormatHandler):
    """Handler for Parquet files."""

    def extension(self) -> str:
        return ".parquet"

    def supports_glob(self) -> bool:
        return True

    def scan(self, url: str, storage_options: dict[str, str] | None = None) -> pl.LazyFrame:
        return pl.scan_parquet(url, storage_options=storage_options)

    def read_stream(self, stream: BinaryIO) -> pl.DataFrame:
        return pl.read_parquet(stream)

    def collect_schema(self, url: str, storage_options: dict[str, str] | None = None) -> list[tuple[str, pl.DataType]]:
        return list(pl.scan_parquet(url, storage_options=storage_options).collect_schema().items())

    def count_rows(self, url: str, storage_options: dict[str, str] | None = None) -> int:
        return pl.scan_parquet(url, storage_options=storage_options).select(pl.len()).collect().item()

    def extra_summary(self, url: str) -> dict[str, str | int | float] | None:
        # TODO: Parquet metadata
        pass

    def write(self, lf: pl.LazyFrame) -> Iterable[bytes]:
        output = BytesIO()
        lf.sink_parquet(output)
        yield output.getvalue()

    def write_to_single_file(self, lf: pl.LazyFrame, path: str) -> None:
        lf.sink_parquet(path)
