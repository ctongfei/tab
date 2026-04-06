import importlib
from io import BytesIO
import sqlite3
import types
from unittest.mock import patch

import polars as pl

from tab_cli.formats.duckdb import DuckdbFormat
from tab_cli.formats.sqlite import SqliteFormat
from tab_cli.handlers.base import TableReader
from tab_cli.storage.aws import AwsAuthMethod, AwsBackend
from tab_cli.storage.az import AzAuthMethod, AzBackend
from tab_cli.storage.base import FileInfo, StorageBackend
from tab_cli.storage.fsspec import CloudFsspecBackend
from tab_cli.storage.local import LocalBackend


class TestAwsStorageOptions:
    def test_storage_options_use_flat_region_keys(self):
        backend = AwsBackend.__new__(AwsBackend)
        backend.method = AwsAuthMethod.EXPLICIT_KEYS
        backend.access_key = "access"
        backend.secret_key = "secret"
        backend.session_token = None
        backend.region = "us-east-1"

        opts = backend.storage_options("s3://bucket/path")

        assert opts is not None
        assert opts["aws_region"] == "us-east-1"
        assert "client_kwargs" not in opts


class TestStorageBackends:
    def test_local_glob_ignores_extension_filter(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        first = data_dir / "part-00000.txt"
        second = data_dir / "part-00001.txt"
        first.write_text("a")
        second.write_text("b")

        glob_path = str(data_dir / "part-*.txt")
        files = list(LocalBackend().list_files(glob_path, "parquet"))

        assert [file_info.url for file_info in files] == [str(first), str(second)]

    def test_local_directory_listing_skips_directories(self, tmp_path):
        data_dir = tmp_path / "data"
        nested_dir = data_dir / "nested"
        nested_dir.mkdir(parents=True)
        file_path = nested_dir / "part-00000.csv"
        file_path.write_text("a,b\n1,2\n", encoding="utf-8")

        files = list(LocalBackend().list_files(str(data_dir), ""))

        assert [file_info.url for file_info in files] == [str(file_path)]


class TestAzureStorageBackend:
    def test_azure_ad_uses_async_default_credential(self, monkeypatch):
        class FakeAsyncCredential:
            async def close(self) -> None:
                return None

        class FakeAzureIdentityAioModule:
            DefaultAzureCredential = FakeAsyncCredential

        class FakeAzureBlobFileSystem:
            def __init__(self, **kwargs):
                self.credential = kwargs.get("credential")

            def ls(self, container):
                return []

        class FakeAdlfsModule:
            AzureBlobFileSystem = FakeAzureBlobFileSystem

        real_import_module = importlib.import_module

        def fake_import_module(name: str):
            if name == "adlfs":
                return FakeAdlfsModule
            if name == "azure.identity.aio":
                return FakeAzureIdentityAioModule
            return real_import_module(name)

        monkeypatch.setattr(importlib, "import_module", fake_import_module)

        backend = AzBackend(account="acct", container="container")

        assert backend.method == AzAuthMethod.AZURE_AD
        assert isinstance(backend.fs.credential, FakeAsyncCredential)


class TestSqliteStorage:
    def test_remote_sqlite_table_is_materialized_locally(self, tmp_path):
        sqlite_path = tmp_path / "remote.db"
        connection = sqlite3.connect(sqlite_path)
        connection.execute("CREATE TABLE people (id INTEGER, name TEXT)")
        connection.execute("INSERT INTO people VALUES (1, 'Ada')")
        connection.commit()
        connection.close()

        sqlite_bytes = sqlite_path.read_bytes()

        class FakeRemoteBackend(StorageBackend):
            def open(self, url: str):
                return BytesIO(sqlite_bytes)

            def list_files(self, url: str, extension: str):
                yield FileInfo(url=url, size=len(sqlite_bytes))

            def size(self, url: str) -> int:
                return len(sqlite_bytes)

            def is_directory(self, url: str) -> bool:
                return False

        with patch.object(
            SqliteFormat,
            "_read_local_query",
            return_value=pl.DataFrame({"id": [1], "name": ["Ada"]}),
        ), patch(
            "tab_cli.formats.sqlite.importlib.import_module",
            return_value=types.SimpleNamespace(),
        ):
            reader = TableReader(FakeRemoteBackend(), SqliteFormat())
            frame = reader.read("s3://bucket/remote.db#people").collect()

        assert frame.to_dicts() == [{"id": 1, "name": "Ada"}]


class TestDuckdbStorage:
    def test_remote_duckdb_table_is_materialized_locally(self, tmp_path):
        duckdb_path = tmp_path / "remote.duckdb"
        duckdb_path.write_bytes(b"duckdb-placeholder")
        duckdb_bytes = duckdb_path.read_bytes()

        class FakeRemoteBackend(StorageBackend):
            def open(self, url: str):
                return BytesIO(duckdb_bytes)

            def list_files(self, url: str, extension: str):
                yield FileInfo(url=url, size=len(duckdb_bytes))

            def size(self, url: str) -> int:
                return len(duckdb_bytes)

            def is_directory(self, url: str) -> bool:
                return False

        with patch.object(
            DuckdbFormat,
            "_read_local_query",
            return_value=pl.DataFrame({"id": [1], "name": ["Ada"]}),
        ), patch(
            "tab_cli.formats.duckdb.importlib.import_module",
            return_value=types.SimpleNamespace(),
        ):
            reader = TableReader(FakeRemoteBackend(), DuckdbFormat())
            frame = reader.read("s3://bucket/remote.duckdb#people").collect()

        assert frame.to_dicts() == [{"id": 1, "name": "Ada"}]


class TestCloudGlobExpansion:
    def test_cloud_glob_uses_segment_expansion_instead_of_fs_glob(self):
        class FakeFs:
            def __init__(self):
                self.glob_called = False

            def ls(self, path: str, detail: bool = False):
                mapping = {
                    "bucket/root": [
                        "bucket/root/date=2026-01-01",
                        "bucket/root/date=2026-01-02",
                        "bucket/root/date=2026-02-01",
                    ],
                    "bucket/root/date=2026-01-01": [
                        "bucket/root/date=2026-01-01/part-000.parquet"
                    ],
                    "bucket/root/date=2026-01-02": [
                        "bucket/root/date=2026-01-02/part-001.parquet"
                    ],
                    "bucket/root/date=2026-02-01": [
                        "bucket/root/date=2026-02-01/part-999.parquet"
                    ],
                }
                return mapping[path]

            def info(self, path: str):
                return {"type": "file", "size": 123}

            def glob(self, pattern: str):
                self.glob_called = True
                raise AssertionError("glob should not be used for segmented cloud patterns")

        class FakeCloudBackend(CloudFsspecBackend):
            protocol = "s3"

            def __init__(self):
                self.fs = FakeFs()

        backend = FakeCloudBackend()

        files = list(backend.list_files("s3://bucket/root/date=2026-01-*/*", "parquet"))

        assert backend.fs.glob_called is False
        assert [file_info.url for file_info in files] == [
            "s3://bucket/root/date=2026-01-01/part-000.parquet",
            "s3://bucket/root/date=2026-01-02/part-001.parquet",
        ]
