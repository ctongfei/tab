from collections.abc import Iterable
import json
import polars as pl
from tab_cli.handlers.base import TableWriter


class JsonlWriter(TableWriter):

    def extension(self) -> str:
        return ".jsonl"

    @staticmethod
    def write(lf: pl.LazyFrame) -> Iterable[bytes]:
        for batch in lf.collect_batches():
            for row in batch.iter_rows(named=True):
                yield (json.dumps(row, default=str, ensure_ascii=False) + "\n").encode("utf-8")

    def write_single(self, lf: pl.LazyFrame, path: str) -> None:
        """Write a LazyFrame to a single JSONL file."""
        with open(path, "wb") as f:
            for chunk in self.write(lf):
                f.write(chunk)
