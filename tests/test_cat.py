import json
import sqlite3
import types
from unittest.mock import patch

import polars as pl

from tab_cli.cli import app
from tab_cli.formats.duckdb import DuckdbFormat
from tab_cli.formats.sqlite import SqliteFormat

from tests.conftest import TEST_CSV, runner


class TestCat:
    def test_basic_outputs_csv(self):
        result = runner.invoke(app, ["cat", TEST_CSV])
        assert result.exit_code == 0
        assert (
            "Participant_ID," in result.output
            or "Participant_ID\t" in result.output
            or "P001" in result.output
        )

    def test_output_format_csv(self):
        result = runner.invoke(app, ["cat", TEST_CSV, "-o", "csv"])
        assert result.exit_code == 0
        lines = result.output.strip().splitlines()
        assert "Participant_ID" in lines[0]
        assert len(lines) == 9

    def test_output_format_tsv(self):
        result = runner.invoke(app, ["cat", TEST_CSV, "-o", "tsv"])
        assert result.exit_code == 0
        lines = result.output.strip().splitlines()
        assert "\t" in lines[0]

    def test_no_rich_table(self):
        result = runner.invoke(app, ["cat", TEST_CSV])
        assert result.exit_code == 0
        assert "─" not in result.output

    def test_rejects_mixed_formats(self, tmp_path):
        jsonl_path = tmp_path / "other.jsonl"
        jsonl_path.write_text('{"value":1}\n', encoding="utf-8")

        result = runner.invoke(app, ["cat", TEST_CSV, str(jsonl_path)])

        assert result.exit_code != 0
        assert result.exception is not None
        assert "must use the same format" in str(result.exception)

    def test_sqlite_requires_explicit_output_format(self, tmp_path):
        sqlite_path = tmp_path / "people.db"
        connection = sqlite3.connect(sqlite_path)
        connection.execute("CREATE TABLE people (id INTEGER, name TEXT)")
        connection.execute("INSERT INTO people VALUES (1, 'Ada')")
        connection.commit()
        connection.close()

        result = runner.invoke(app, ["cat", f"{sqlite_path}#people"])

        assert result.exit_code != 0
        assert result.exception is not None
        assert "Output format" in str(result.exception)

    def test_cat_sqlite_with_csv_output(self, tmp_path):
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
            result = runner.invoke(app, ["cat", f"{sqlite_path}#people", "-o", "csv"])

        assert result.exit_code == 0
        assert "id,name" in result.output
        assert "Ada" in result.output

    def test_duckdb_requires_explicit_output_format(self, tmp_path):
        duckdb_path = tmp_path / "people.duckdb"
        duckdb_path.write_bytes(b"duckdb-placeholder")

        result = runner.invoke(app, ["cat", f"{duckdb_path}#people"])

        assert result.exit_code != 0
        assert result.exception is not None
        assert "Output format" in str(result.exception)

    def test_cat_duckdb_with_csv_output(self, tmp_path):
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
            result = runner.invoke(app, ["cat", f"{duckdb_path}#people", "-o", "csv"])

        assert result.exit_code == 0
        assert "id,name" in result.output
        assert "Ada" in result.output


class TestSqlOption:
    def test_view_with_sql(self):
        result = runner.invoke(
            app,
            ["view", TEST_CSV, "--sql", "SELECT * FROM t WHERE Status = 'Baseline'"],
        )
        assert result.exit_code == 0
        assert "Baseline" in result.output
        assert "Active" not in result.output

    def test_view_with_sql_and_limit(self):
        result = runner.invoke(
            app, ["view", TEST_CSV, "--sql", "SELECT * FROM t", "--limit", "2"]
        )
        assert result.exit_code == 0
        count = sum(1 for line in result.output.splitlines() if "P00" in line)
        assert count <= 2

    def test_cat_with_sql_and_output_format(self):
        result = runner.invoke(
            app,
            [
                "cat",
                TEST_CSV,
                "--sql",
                "SELECT Participant_ID, Status FROM t",
                "-o",
                "csv",
            ],
        )
        assert result.exit_code == 0
        lines = result.output.strip().splitlines()
        assert "Participant_ID" in lines[0]
        assert "Status" in lines[0]


class TestJmespathOption:
    def test_view_with_jmespath_object(self):
        result = runner.invoke(
            app, ["view", TEST_CSV, "--jp", "{id: Participant_ID, status: Status}"]
        )
        assert result.exit_code == 0
        assert "id" in result.output
        assert "status" in result.output
        assert "Baseline" in result.output

    def test_cat_with_jmespath_object_output(self):
        result = runner.invoke(
            app,
            [
                "cat",
                TEST_CSV,
                "--jp",
                "{id: Participant_ID, status: Status}",
                "-o",
                "jsonl",
            ],
        )
        assert result.exit_code == 0
        first_row = json.loads(result.output.strip().splitlines()[0])
        assert first_row == {"id": "P001", "status": "Baseline"}

    def test_cat_with_jmespath_scalar_output(self):
        result = runner.invoke(
            app, ["cat", TEST_CSV, "--jp", "Participant_ID", "-o", "jsonl"]
        )
        assert result.exit_code == 0
        first_row = json.loads(result.output.strip().splitlines()[0])
        assert first_row == {"value": "P001"}

    def test_cat_with_jmespath_list_output(self):
        result = runner.invoke(
            app,
            ["cat", TEST_CSV, "--jp", "[Participant_ID, Status]", "-o", "jsonl"],
        )
        assert result.exit_code == 0
        first_row = json.loads(result.output.strip().splitlines()[0])
        assert first_row == {"value": ["P001", "Baseline"]}

    def test_cat_with_jmespath_null_output(self):
        result = runner.invoke(
            app, ["cat", TEST_CSV, "--jp", "MissingField", "-o", "jsonl"]
        )
        assert result.exit_code == 0
        first_row = json.loads(result.output.strip().splitlines()[0])
        assert first_row == {"value": None}

    def test_sql_and_jmespath_are_mutually_exclusive(self):
        result = runner.invoke(
            app,
            [
                "cat",
                TEST_CSV,
                "--sql",
                "SELECT * FROM t",
                "--jp",
                "Participant_ID",
                "-o",
                "jsonl",
            ],
        )
        assert result.exit_code != 0
        assert result.exception is not None
        assert "At most one query may be provided" in str(result.exception)
