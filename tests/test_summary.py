import io
import sqlite3
import threading
import time
import types
from unittest.mock import patch

import polars as pl

from tab_cli import config as config_module
from tab_cli.cli import app
from tab_cli.formats.base import FormatHandler
from tab_cli.formats.duckdb import DuckdbFormat
from tab_cli.formats.sqlite import SqliteFormat
from tab_cli.handlers.base import TableReader
from tab_cli.storage.base import FileInfo, StorageBackend

from tests.conftest import runner


class TestSummary:
    def test_remote_directory_summary_uses_multiple_workers(self):
        thread_ids: set[int] = set()
        lock = threading.Lock()

        class FakeRemoteBackend(StorageBackend):
            def open(self, url: str):
                return io.BytesIO()

            def list_files(self, url: str, extension: str):
                for index in range(4):
                    yield FileInfo(
                        url=f"remote://dataset/part-{index:05d}{extension}",
                        size=10,
                    )

            def size(self, url: str) -> int:
                return 40

            def is_directory(self, url: str) -> bool:
                return True

            def normalize_for_polars(self, url: str) -> str:
                return url

        class FakeFormat(FormatHandler):
            def extension(self) -> str:
                return "parquet"

            def scan(self, url: str, storage_options: dict[str, str] | None = None) -> pl.LazyFrame:
                raise AssertionError("scan should not be called")

            def read_stream(self, stream) -> pl.DataFrame:
                raise AssertionError("read_stream should not be called")

            def collect_schema(
                self,
                url: str,
                storage_options: dict[str, str] | None = None,
            ) -> list[tuple[str, pl.DataType]]:
                return [("value", pl.Int64)]

            def count_rows(
                self,
                url: str,
                storage_options: dict[str, str] | None = None,
                opener=None,
            ) -> int:
                with lock:
                    thread_ids.add(threading.get_ident())
                time.sleep(0.05)
                return 1

            def write(self, lf: pl.LazyFrame):
                raise AssertionError("write should not be called")

            def write_to_single_file(self, lf: pl.LazyFrame, path: str) -> None:
                raise AssertionError("write_to_single_file should not be called")

        original_workers = config_module.config.num_remote_workers
        config_module.config.num_remote_workers = 4
        try:
            summary = TableReader(FakeRemoteBackend(), FakeFormat()).summary(
                "remote://dataset"
            )
        finally:
            config_module.config.num_remote_workers = original_workers

        assert summary.num_rows == 4
        assert summary.num_columns == 1
        assert summary.extra == {"Partitions": 4}
        assert len(thread_ids) > 1

    def test_summary_supports_glob_input(self, tmp_path):
        dataset_dir = tmp_path / "dataset"
        for partition in range(2):
            part_dir = dataset_dir / f"date=2026-01-0{partition + 1}"
            part_dir.mkdir(parents=True)
            pl.DataFrame({"value": [partition, partition + 10]}).write_parquet(
                part_dir / f"part-{partition:05d}.parquet"
            )

        glob_path = str(dataset_dir / "date=*" / "*.parquet")
        result = runner.invoke(app, ["summary", glob_path])

        assert result.exit_code == 0
        assert "Partitions" in result.output
        assert "4" in result.output
        assert "2" in result.output

    def test_summary_rejects_inconsistent_schema(self):
        class FakeBackend(StorageBackend):
            def open(self, url: str):
                return io.BytesIO()

            def list_files(self, url: str, extension: str):
                yield FileInfo(url="memory://part-1.parquet", size=10)
                yield FileInfo(url="memory://part-2.parquet", size=10)

            def size(self, url: str) -> int:
                return 20

            def is_directory(self, url: str) -> bool:
                return True

            def normalize_for_polars(self, url: str) -> str:
                return url

        class FakeFormat(FormatHandler):
            def extension(self) -> str:
                return "parquet"

            def scan(self, url: str, storage_options: dict[str, str] | None = None) -> pl.LazyFrame:
                raise AssertionError("scan should not be called")

            def read_stream(self, stream) -> pl.DataFrame:
                raise AssertionError("read_stream should not be called")

            def collect_schema(
                self,
                url: str,
                storage_options: dict[str, str] | None = None,
            ) -> list[tuple[str, pl.DataType]]:
                if url.endswith("part-1.parquet"):
                    return [("value", pl.Int64)]
                return [("other", pl.Int64)]

            def count_rows(
                self,
                url: str,
                storage_options: dict[str, str] | None = None,
                opener=None,
            ) -> int:
                return 1

            def write(self, lf: pl.LazyFrame):
                raise AssertionError("write should not be called")

            def write_to_single_file(self, lf: pl.LazyFrame, path: str) -> None:
                raise AssertionError("write_to_single_file should not be called")

        try:
            TableReader(FakeBackend(), FakeFormat()).summary("memory://dataset")
            assert False, "Expected ValueError"
        except ValueError as exc:
            assert "Inconsistent schema" in str(exc)

    def test_summary_sqlite_table(self, tmp_path):
        sqlite_path = tmp_path / "people.db"
        connection = sqlite3.connect(sqlite_path)
        connection.execute("CREATE TABLE people (id INTEGER, name TEXT)")
        connection.execute("INSERT INTO people VALUES (1, 'Ada')")
        connection.execute("INSERT INTO people VALUES (2, 'Grace')")
        connection.commit()
        connection.close()

        with patch.object(
            SqliteFormat,
            "_read_local_query",
            side_effect=[
                pl.DataFrame({"row_count": [2]}),
                pl.DataFrame({"id": [], "name": []}),
            ],
        ), patch(
            "tab_cli.formats.sqlite.importlib.import_module",
            return_value=types.SimpleNamespace(),
        ):
            result = runner.invoke(app, ["summary", f"{sqlite_path}#people"])

        assert result.exit_code == 0
        assert "Rows" in result.output
        assert "2" in result.output

    def test_summary_duckdb_table(self, tmp_path):
        duckdb_path = tmp_path / "people.duckdb"
        duckdb_path.write_bytes(b"duckdb-placeholder")

        with patch.object(
            DuckdbFormat,
            "_read_local_query",
            side_effect=[
                pl.DataFrame({"row_count": [2]}),
                pl.DataFrame({"id": [], "name": []}),
            ],
        ), patch(
            "tab_cli.formats.duckdb.importlib.import_module",
            return_value=types.SimpleNamespace(),
        ):
            result = runner.invoke(app, ["summary", f"{duckdb_path}#people"])

        assert result.exit_code == 0
        assert "Rows" in result.output
        assert "2" in result.output
