from unittest.mock import patch
import sqlite3
import types

import polars as pl

from tab_cli.cli import app
from tab_cli.formats import parquet as parquet_module
from tab_cli.formats.duckdb import DuckdbFormat
from tab_cli.formats.parquet import ParquetFormat
from tab_cli.formats.sqlite import SqliteFormat
from tab_cli.handlers import infer_reader

from tests.conftest import TEST_CSV, runner


class TestView:
    def test_basic(self):
        result = runner.invoke(app, ["view", TEST_CSV])
        assert result.exit_code == 0
        assert "P001" in result.output
        assert "Control" in result.output

    def test_limit(self):
        result = runner.invoke(app, ["view", TEST_CSV, "--limit", "2"])
        assert result.exit_code == 0
        assert "P001" in result.output
        assert "P003" not in result.output
        assert "..." not in result.output

    def test_skip(self):
        result = runner.invoke(app, ["view", TEST_CSV, "--skip", "6", "--limit", "10"])
        assert result.exit_code == 0
        assert "P001" not in result.output
        assert "P004" in result.output

    def test_max_cell_length(self):
        result = runner.invoke(app, ["view", TEST_CSV, "--max-cell-length", "5"])
        assert result.exit_code == 0
        assert "Contr..." in result.output
        assert "P001" in result.output

    def test_cell_values_are_rendered_verbatim_when_they_look_like_markup(self, tmp_path):
        csv_path = tmp_path / "markup.csv"
        csv_path.write_text("text\n[red]literal[/red]\n", encoding="utf-8")

        result = runner.invoke(app, ["view", str(csv_path)])

        assert result.exit_code == 0
        assert "[red]literal[/red]" in result.output

    def test_no_output_flag(self):
        result = runner.invoke(app, ["view", TEST_CSV, "-o", "csv"])
        assert result.exit_code != 0

    def test_truncation_indicator(self):
        result = runner.invoke(app, ["view", TEST_CSV])
        assert result.exit_code == 0
        lines_with_ellipsis = [
            line
            for line in result.output.splitlines()
            if line.strip() == "...   ...   ...   ...   ...   ..."
        ]
        assert len(lines_with_ellipsis) == 0

    def test_directory_preview_reads_only_needed_parquet_partitions(self, tmp_path):
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()
        for partition in range(3):
            pl.DataFrame(
                {
                    "partition": [partition] * 12,
                    "row": list(range(partition * 12, partition * 12 + 12)),
                }
            ).write_parquet(dataset_dir / f"part-{partition:05d}.parquet")

        scanned_urls: list[str] = []
        original_scan = ParquetFormat.scan

        def tracking_scan(
            self,
            url: str,
            storage_options: dict[str, str] | None = None,
        ) -> pl.LazyFrame:
            scanned_urls.append(url)
            return original_scan(self, url, storage_options=storage_options)

        with patch.object(ParquetFormat, "scan", tracking_scan):
            result = runner.invoke(app, ["view", str(dataset_dir)])

        assert result.exit_code == 0
        assert scanned_urls == [
            str(dataset_dir / "part-00000.parquet"),
            str(dataset_dir / "part-00001.parquet"),
        ]

    def test_glob_preview_reads_only_needed_parquet_matches(self, tmp_path):
        dataset_dir = tmp_path / "dataset"
        for partition in range(3):
            part_dir = dataset_dir / f"date=2026-01-0{partition + 1}"
            part_dir.mkdir(parents=True)
            pl.DataFrame(
                {
                    "partition": [partition] * 12,
                    "row": list(range(partition * 12, partition * 12 + 12)),
                }
            ).write_parquet(part_dir / f"part-{partition:05d}.parquet")

        scanned_urls: list[str] = []
        original_scan = ParquetFormat.scan

        def tracking_scan(
            self,
            url: str,
            storage_options: dict[str, str] | None = None,
        ) -> pl.LazyFrame:
            scanned_urls.append(url)
            return original_scan(self, url, storage_options=storage_options)

        glob_path = str(dataset_dir / "date=*" / "*.parquet")
        with patch.object(ParquetFormat, "scan", tracking_scan):
            result = runner.invoke(app, ["view", glob_path])

        assert result.exit_code == 0
        assert scanned_urls == [
            str(dataset_dir / "date=2026-01-01" / "part-00000.parquet"),
            str(dataset_dir / "date=2026-01-02" / "part-00001.parquet"),
        ]

    def test_directory_preview_respects_skip_and_limit(self, tmp_path):
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()
        for partition in range(3):
            pl.DataFrame(
                {
                    "value": list(range(partition * 10, partition * 10 + 10)),
                }
            ).write_parquet(dataset_dir / f"part-{partition:05d}.parquet")

        scanned_urls: list[str] = []
        original_scan = ParquetFormat.scan

        def tracking_scan(
            self,
            url: str,
            storage_options: dict[str, str] | None = None,
        ) -> pl.LazyFrame:
            scanned_urls.append(url)
            return original_scan(self, url, storage_options=storage_options)

        with patch.object(ParquetFormat, "scan", tracking_scan):
            result = runner.invoke(
                app,
                ["view", str(dataset_dir), "--skip", "11", "--limit", "3"],
            )

        assert result.exit_code == 0
        assert scanned_urls == [
            str(dataset_dir / "part-00000.parquet"),
            str(dataset_dir / "part-00001.parquet"),
        ]
        assert "11" in result.output
        assert "12" in result.output
        assert "13" in result.output
        assert "14" not in result.output

    def test_reader_read_keeps_generic_directory_scan(self, tmp_path):
        dataset_dir = tmp_path / "dataset"
        dataset_dir.mkdir()
        for partition in range(3):
            pl.DataFrame({"value": [partition]}).write_parquet(
                dataset_dir / f"part-{partition:05d}.parquet"
            )

        scanned_urls: list[str] = []
        original_scan = ParquetFormat.scan

        def tracking_scan(
            self,
            url: str,
            storage_options: dict[str, str] | None = None,
        ) -> pl.LazyFrame:
            scanned_urls.append(url)
            return original_scan(self, url, storage_options=storage_options)

        with patch.object(ParquetFormat, "scan", tracking_scan):
            reader = infer_reader(str(dataset_dir))
            reader.read(str(dataset_dir)).collect()

        assert scanned_urls == [
            str(dataset_dir / "part-00000.parquet"),
            str(dataset_dir / "part-00001.parquet"),
            str(dataset_dir / "part-00002.parquet"),
        ]

    def test_parquet_count_rows_uses_metadata(self, tmp_path):
        parquet_path = tmp_path / "rows.parquet"
        pl.DataFrame({"value": [1, 2, 3, 4]}).write_parquet(parquet_path)

        with patch.object(
            parquet_module,
            "_scan_parquet_with_pyarrow_fallback",
            side_effect=AssertionError("count_rows should not scan parquet data"),
        ):
            assert ParquetFormat().count_rows(str(parquet_path)) == 4

    def test_view_sqlite_table(self, tmp_path):
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
            return_value=pl.DataFrame({"id": [1, 2], "name": ["Ada", "Grace"]}),
        ), patch(
            "tab_cli.formats.sqlite.importlib.import_module",
            return_value=types.SimpleNamespace(),
        ):
            result = runner.invoke(app, ["view", f"{sqlite_path}#people"])

        assert result.exit_code == 0
        assert "Ada" in result.output
        assert "Grace" in result.output

    def test_view_duckdb_table(self, tmp_path):
        duckdb_path = tmp_path / "people.duckdb"
        duckdb_path.write_bytes(b"duckdb-placeholder")

        with patch.object(
            DuckdbFormat,
            "_read_local_query",
            return_value=pl.DataFrame({"id": [1, 2], "name": ["Ada", "Grace"]}),
        ), patch(
            "tab_cli.formats.duckdb.importlib.import_module",
            return_value=types.SimpleNamespace(),
        ):
            result = runner.invoke(app, ["view", f"{duckdb_path}#people"])

        assert result.exit_code == 0
        assert "Ada" in result.output
        assert "Grace" in result.output
