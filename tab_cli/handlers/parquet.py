"""Parquet file handler using Polars."""

import os
from collections.abc import Iterable
from io import BytesIO

import polars as pl

from tab_cli.handlers.base import TableReader, TableWriter, TableSchema, TableSummary


class ParquetHandler(TableReader, TableWriter):
    """Handler for Parquet files."""

    def extension(self) -> str:
        return ".parquet"

    @staticmethod
    def read(path: str, limit: int | None = None, offset: int = 0) -> pl.LazyFrame:
        """Read data from a Parquet file."""
        df = pl.scan_parquet(path)

        if offset > 0:
            df = df.slice(offset, length=limit)
        elif limit is not None:
            df = df.head(limit)

        return df

    @staticmethod
    def schema(path: str) -> TableSchema:
        """Get the schema of the Parquet file."""
        lf = pl.scan_parquet(path)
        columns = list(lf.collect_schema().items())
        return TableSchema(columns=columns)

    @staticmethod
    def summary(path: str) -> TableSummary:
        """Get summary information about the Parquet file."""
        import pyarrow.parquet as pq

        file_size = os.path.getsize(path)
        lf = pl.scan_parquet(path)
        schema = lf.collect_schema()
        num_columns = len(schema)
        num_rows = lf.select(pl.len()).collect().item()

        # Get parquet metadata using pyarrow
        pf = pq.ParquetFile(path)
        metadata = pf.metadata

        extra: dict[str, str | int | float] = {}

        # Collect compression codecs from all column chunks
        codecs: set[str] = set()
        for rg_idx in range(metadata.num_row_groups):
            rg = metadata.row_group(rg_idx)
            for col_idx in range(rg.num_columns):
                col = rg.column(col_idx)
                codecs.add(col.compression)
        extra["Row groups"] = metadata.num_row_groups
        if codecs:
            extra["Compression"] = ", ".join(sorted(codecs))

        return TableSummary(
            file_size=file_size,
            num_rows=num_rows,
            num_columns=num_columns,
            extra=extra,
        )

    @staticmethod
    def write(lf: pl.LazyFrame) -> Iterable[bytes]:
        """Write a LazyFrame to Parquet bytes."""
        output = BytesIO()
        lf.sink_parquet(output)
        yield output.getvalue()

    def write_single(self, lf: pl.LazyFrame, path: str) -> None:
        """Write a LazyFrame to a single Parquet file."""
        lf.sink_parquet(path)
