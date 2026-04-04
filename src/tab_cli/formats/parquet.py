"""Parquet format handler."""

from collections.abc import Iterable
from io import BytesIO
from typing import BinaryIO, Callable

from loguru import logger
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from tab_cli.formats.base import FormatHandler


def _scan_parquet_with_pyarrow_fallback(
    url: str,
    storage_options: dict[str, str] | None = None,
) -> pl.LazyFrame:
    """Scan a Parquet file, falling back to PyArrow reader on failure.

    Polars' native Parquet reader is stricter than PyArrow and may reject
    files with schema inconsistencies (e.g., mixed struct schemas across
    row groups, legacy encodings, INT96 timestamps). When this happens,
    we retry with `use_pyarrow=True` which delegates decoding to PyArrow.
    """
    try:
        lf = pl.scan_parquet(url, storage_options=storage_options)
        lf.collect_schema()  # force schema resolution to catch errors early
        return lf
    except Exception as e:
        logger.warning(
            f"Polars native Parquet reader failed ({e}), retrying with PyArrow backend"
        )
        return pl.read_parquet(url, storage_options=storage_options, use_pyarrow=True).lazy()


class ParquetFormat(FormatHandler):
    """Handler for Parquet files."""

    def extension(self) -> str:
        return "parquet"

    def supports_glob(self) -> bool:
        return True

    def scan(self, url: str, storage_options: dict[str, str] | None = None) -> pl.LazyFrame:
        return _scan_parquet_with_pyarrow_fallback(url, storage_options=storage_options)

    def read_stream(self, stream: BinaryIO) -> pl.DataFrame:
        return pl.read_parquet(stream)

    def collect_schema(self, url: str, storage_options: dict[str, str] | None = None) -> list[tuple[str, pl.DataType]]:
        return list(_scan_parquet_with_pyarrow_fallback(url, storage_options=storage_options).collect_schema().items())

    def count_rows(
        self,
        url: str,
        storage_options: dict[str, str] | None = None,
        opener: Callable[[str], BinaryIO] | None = None,
    ) -> int:
        if opener is not None:
            with opener(url) as stream:
                return pq.ParquetFile(pa.PythonFile(stream, mode="r")).metadata.num_rows
        with open(url, "rb") as stream:
            return pq.ParquetFile(pa.PythonFile(stream, mode="r")).metadata.num_rows

    def extra_summary(self, url: str) -> dict[str, str | int | float] | None:
        return None

    def write(self, lf: pl.LazyFrame) -> Iterable[bytes]:
        output = BytesIO()
        lf.sink_parquet(output)
        yield output.getvalue()

    def write_to_single_file(self, lf: pl.LazyFrame, path: str) -> None:
        lf.sink_parquet(path)
