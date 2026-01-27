"""CSV file handler using Polars."""

import os
from collections.abc import Iterable
from io import BytesIO

import polars as pl

from tab_cli.handlers.base import TableReader, TableWriter, TableSchema, TableSummary


class CsvHandler(TableReader, TableWriter):
    """Handler for CSV/TSV files."""

    def __init__(self, separator: str = ","):
        self.separator = separator

    def extension(self) -> str:
        return ".csv" if self.separator == "," else ".tsv"

    def read(self, path: str, limit: int | None = None, offset: int = 0) -> pl.LazyFrame:
        lf = pl.scan_csv(path, separator=self.separator)
        if offset > 0:
            lf = lf.slice(offset, length=limit)
        elif limit is not None:
            lf = lf.head(limit)
        return lf

    def schema(self, path: str) -> TableSchema:
        lf = pl.scan_csv(path, separator=self.separator)
        columns = list(lf.collect_schema().items())
        return TableSchema(columns=columns)

    def summary(self, path: str) -> TableSummary:
        file_size = os.path.getsize(path)
        lf = pl.scan_csv(path, separator=self.separator)
        schema = lf.collect_schema()
        num_columns = len(schema)
        num_rows = lf.select(pl.len()).collect().item()
        return TableSummary(
            file_size=file_size,
            num_rows=num_rows,
            num_columns=num_columns,
        )

    def write(self, lf: pl.LazyFrame) -> Iterable[bytes]:
        first = True
        for batch in lf.collect_batches():
            output = BytesIO()
            batch.write_csv(output, separator=self.separator, include_header=first)
            first = False
            yield output.getvalue()

    def write_single(self, lf: pl.LazyFrame, path: str) -> None:
        """Write a LazyFrame to a single CSV/TSV file."""
        lf.sink_csv(path, separator=self.separator)
