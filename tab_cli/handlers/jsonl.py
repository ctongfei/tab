import os
from collections.abc import Iterable
import json
import polars as pl
from tab_cli.handlers.base import TableReader, TableWriter, TableSchema, TableSummary


class JsonlHandler(TableReader, TableWriter):

    def read(self, path: str, limit: int | None = None, offset: int = 0) -> pl.LazyFrame:
        lf = pl.scan_ndjson(path)
        if offset > 0:
            lf = lf.slice(offset, length=limit)
        elif limit is not None:
            lf = lf.head(limit)
        return lf

    def schema(self, path: str) -> TableSchema:
        lf = pl.scan_ndjson(path)
        columns = list(lf.collect_schema().items())
        return TableSchema(columns=columns)

    def summary(self, path: str) -> TableSummary:
        file_size = os.path.getsize(path)
        lf = pl.scan_ndjson(path)
        schema = lf.collect_schema()
        num_columns = len(schema)
        num_rows = lf.select(pl.len()).collect().item()
        return TableSummary(
            file_size=file_size,
            num_rows=num_rows,
            num_columns=num_columns,
        )

    def extension(self) -> str:
        return ".jsonl"

    def write(self, lf: pl.LazyFrame) -> Iterable[bytes]:
        for batch in lf.collect_batches():
            for row in batch.iter_rows(named=True):
                yield (json.dumps(row, default=str, ensure_ascii=False) + "\n").encode("utf-8")

    def write_single(self, lf: pl.LazyFrame, path: str) -> None:
        """Write a LazyFrame to a single JSONL file."""
        with open(path, "wb") as f:
            for chunk in self.write(lf):
                f.write(chunk)
