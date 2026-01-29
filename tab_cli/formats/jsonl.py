"""JSONL (newline-delimited JSON) format handler."""

import json
from collections.abc import Iterable
from typing import BinaryIO

import polars as pl

from tab_cli.formats.base import FormatHandler


class JsonlFormat(FormatHandler):
    """Handler for JSONL files."""

    def extension(self) -> str:
        return ".jsonl"

    def supports_glob(self) -> bool:
        return True

    def scan(self, url: str, storage_options: dict[str, str] | None = None) -> pl.LazyFrame:
        return pl.scan_ndjson(url, storage_options=storage_options)

    def read_stream(self, stream: BinaryIO) -> pl.DataFrame:
        return pl.read_ndjson(stream)

    def collect_schema(self, url: str, storage_options: dict[str, str] | None = None) -> list[tuple[str, pl.DataType]]:
        return list(pl.scan_ndjson(url, storage_options=storage_options).collect_schema().items())

    def count_rows(self, url: str, storage_options: dict[str, str] | None = None) -> int:
        return pl.scan_ndjson(url, storage_options=storage_options).select(pl.len()).collect().item()

    def write(self, lf: pl.LazyFrame) -> Iterable[bytes]:
        for batch in lf.collect_batches():
            for row in batch.iter_rows(named=True):
                yield (json.dumps(row, default=str, ensure_ascii=False) + "\n").encode("utf-8")

    def write_to_single_file(self, lf: pl.LazyFrame, path: str) -> None:
        with open(path, "wb") as f:
            for chunk in self.write(lf):
                f.write(chunk)
