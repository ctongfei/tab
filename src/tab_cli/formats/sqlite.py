"""SQLite format handler."""

from collections.abc import Iterable
import importlib
import os
import shutil
from tempfile import NamedTemporaryFile
from typing import BinaryIO, Callable

import polars as pl

from tab_cli.formats.base import FormatHandler
from tab_cli.url_parser import parse_url


def split_sqlite_url(url: str) -> tuple[str, str]:
    """Split a SQLite input URL into database URL and table name."""
    db_url, separator, table_name = url.rpartition("#")
    if separator == "":
        raise ValueError(
            "SQLite input must be specified as {url}#{table_name}, for example data.db#users"
        )
    if len(db_url) == 0:
        raise ValueError("SQLite input must include a database URL before '#'")
    if len(table_name) == 0:
        raise ValueError("SQLite input must include a table name after '#'")
    return db_url, table_name


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


class SqliteFormat(FormatHandler):
    """Handler for SQLite database tables."""

    def extension(self) -> str:
        return "sqlite"

    def source_url(self, url: str) -> str:
        db_url, _ = split_sqlite_url(url)
        return db_url

    def uses_normalized_url(self) -> bool:
        return False

    def supports_multi_file(self) -> bool:
        return False

    def needs_opener(self) -> bool:
        return True

    def scan(
        self,
        url: str,
        storage_options: dict[str, str] | None = None,
        opener: Callable[[str], BinaryIO] | None = None,
    ) -> pl.LazyFrame:
        _, table_name = split_sqlite_url(url)
        query = f"SELECT * FROM {_quote_identifier(table_name)}"
        return self._read_query(url, query, opener=opener).lazy()

    def read_stream(self, stream: BinaryIO) -> pl.DataFrame:
        raise ValueError("SQLite input does not support stdin; pass a database path as {url}#{table_name}")

    def collect_schema(
        self,
        url: str,
        storage_options: dict[str, str] | None = None,
        opener: Callable[[str], BinaryIO] | None = None,
    ) -> list[tuple[str, pl.DataType]]:
        _, table_name = split_sqlite_url(url)
        query = f"SELECT * FROM {_quote_identifier(table_name)} LIMIT 0"
        return list(self._read_query(url, query, opener=opener).schema.items())

    def count_rows(
        self,
        url: str,
        storage_options: dict[str, str] | None = None,
        opener: Callable[[str], BinaryIO] | None = None,
    ) -> int:
        _, table_name = split_sqlite_url(url)
        query = f"SELECT COUNT(*) AS row_count FROM {_quote_identifier(table_name)}"
        return int(self._read_query(url, query, opener=opener).item())

    def write(self, lf: pl.LazyFrame) -> Iterable[bytes]:
        raise ValueError("SQLite output is not supported")

    def write_to_single_file(self, lf: pl.LazyFrame, path: str) -> None:
        raise ValueError("SQLite output is not supported")

    def _read_query(
        self,
        url: str,
        query: str,
        opener: Callable[[str], BinaryIO] | None = None,
    ) -> pl.DataFrame:
        try:
            importlib.import_module("adbc_driver_sqlite")
        except ImportError as e:
            raise ImportError(
                "Package 'adbc-driver-sqlite' is required for SQLite input. "
                "Install with: pip install 'tab-cli[sqlite]'"
            ) from e

        db_url = self.source_url(url)
        parsed = parse_url(db_url)
        if parsed.scheme == "file":
            return self._read_local_query(parsed.path, query)

        if opener is None:
            raise ValueError("SQLite remote input requires a storage backend opener")

        with opener(db_url) as stream:
            with NamedTemporaryFile(suffix=".db", delete=False) as temp_file:
                shutil.copyfileobj(stream, temp_file)
                temp_path = temp_file.name

        try:
            return self._read_local_query(temp_path, query)
        finally:
            os.unlink(temp_path)

    def _read_local_query(self, path: str, query: str) -> pl.DataFrame:
        sqlite_path = os.path.abspath(path)
        return pl.read_database_uri(
            query=query,
            uri=f"sqlite:///{sqlite_path}",
            engine="adbc",
        )
