"""CSV/TSV format handler."""

from collections.abc import Iterable
from io import BytesIO
from typing import BinaryIO

import polars as pl

from tab_cli.formats.base import FormatHandler


class CsvFormat(FormatHandler):
    """Handler for CSV/TSV files."""

    def __init__(self, separator: str = ","):
        self.separator = separator

    def extension(self) -> str:
        return ".csv" if self.separator == "," else ".tsv"

    def supports_glob(self) -> bool:
        return True

    def scan(self, url: str, storage_options: dict[str, str] | None = None) -> pl.LazyFrame:
        return pl.scan_csv(url, separator=self.separator, storage_options=storage_options)

    def read_stream(self, stream: BinaryIO) -> pl.DataFrame:
        return pl.read_csv(stream, separator=self.separator)

    def collect_schema(self, url: str, storage_options: dict[str, str] | None = None) -> list[tuple[str, pl.DataType]]:
        return list(pl.scan_csv(url, separator=self.separator, storage_options=storage_options).collect_schema().items())

    def count_rows(self, url: str, storage_options: dict[str, str] | None = None) -> int:
        return pl.scan_csv(url, separator=self.separator, storage_options=storage_options).select(pl.len()).collect().item()

    def write(self, lf: pl.LazyFrame) -> Iterable[bytes]:
        first = True
        for batch in lf.collect_batches():
            output = BytesIO()
            batch.write_csv(output, separator=self.separator, include_header=first)
            first = False
            yield output.getvalue()

    def write_to_single_file(self, lf: pl.LazyFrame, path: str) -> None:
        lf.sink_csv(path, separator=self.separator)
