"""Avro file handler using polars-fastavro."""

import os
from collections.abc import Iterable
from io import BytesIO

import polars as pl
import polars_fastavro

from tab_cli.handlers.base import TableReader, TableSchema, TableSummary, TableWriter


class AvroHandler(TableReader, TableWriter):
    """Handler for Avro files."""

    def extension(self) -> str:
        return ".avro"

    def read(self, path: str, limit: int | None = None, offset: int = 0) -> pl.LazyFrame:
        lf = polars_fastavro.scan_avro(path)
        if offset > 0:
            lf = lf.slice(offset, length=limit)
        elif limit is not None:
            lf = lf.head(limit)
        return lf

    def schema(self, path: str) -> TableSchema:
        lf = polars_fastavro.scan_avro(path)
        columns = list(lf.collect_schema().items())
        return TableSchema(columns=columns)

    def summary(self, path: str) -> TableSummary:
        file_size = os.path.getsize(path)
        lf = polars_fastavro.scan_avro(path)
        schema = lf.collect_schema()
        num_columns = len(schema)
        num_rows = lf.select(pl.len()).collect().item()
        return TableSummary(
            file_size=file_size,
            num_rows=num_rows,
            num_columns=num_columns,
        )

    def write(self, lf: pl.LazyFrame) -> Iterable[bytes]:
        output = BytesIO()
        df = lf.collect()
        polars_fastavro.write_avro(df, output)
        yield output.getvalue()

    def write_single(self, lf: pl.LazyFrame, path: str) -> None:
        df = lf.collect()
        polars_fastavro.write_avro(df, path)
