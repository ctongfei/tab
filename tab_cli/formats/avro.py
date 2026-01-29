"""Avro format handler using polars-fastavro."""

from collections.abc import Iterable
from io import BytesIO
from typing import BinaryIO

import polars as pl
import polars_fastavro

from tab_cli.formats.base import FormatHandler


class AvroFormat(FormatHandler):
    """Handler for Avro files."""

    def extension(self) -> str:
        return ".avro"

    def supports_glob(self) -> bool:
        # polars_fastavro doesn't support glob patterns
        return False

    def scan(self, url: str, storage_options: dict[str, str] | None = None) -> pl.LazyFrame:
        # polars_fastavro doesn't support storage_options, so cloud URIs
        # need to be accessed through fsspec first
        return polars_fastavro.scan_avro(url)

    def read_stream(self, stream: BinaryIO) -> pl.DataFrame:
        return polars_fastavro.read_avro(stream)

    def collect_schema(self, url: str, storage_options: dict[str, str] | None = None) -> list[tuple[str, pl.DataType]]:
        # polars_fastavro doesn't support storage_options
        return list(polars_fastavro.scan_avro(url).collect_schema().items())

    def count_rows(self, url: str, storage_options: dict[str, str] | None = None) -> int:
        # polars_fastavro doesn't support storage_options
        return polars_fastavro.scan_avro(url).select(pl.len()).collect().item()

    def write(self, lf: pl.LazyFrame) -> Iterable[bytes]:
        output = BytesIO()
        df = lf.collect()
        polars_fastavro.write_avro(df, output)
        yield output.getvalue()

    def write_to_single_file(self, lf: pl.LazyFrame, path: str) -> None:
        df = lf.collect()
        polars_fastavro.write_avro(df, path)
