"""Tests for the tab CLI commands."""

import json
import os
from unittest.mock import patch

from typer.testing import CliRunner

from tab_cli import config as config_module
from tab_cli.cli import app
from tab_cli.config import Config, load_config_file

runner = CliRunner()
TEST_CSV = os.path.join(os.path.dirname(__file__), "assets", "test.csv")

# Read test CSV content for stdin tests
with open(TEST_CSV, "rb") as _f:
    TEST_CSV_BYTES = _f.read()
TEST_CSV_TEXT = TEST_CSV_BYTES.decode("utf-8")


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
        # Row 3 (P002 second row) should not appear
        assert "P003" not in result.output
        # No truncation indicator when explicit limit
        assert "..." not in result.output

    def test_skip(self):
        result = runner.invoke(app, ["view", TEST_CSV, "--skip", "6", "--limit", "10"])
        assert result.exit_code == 0
        # First 6 rows skipped; only P004 rows remain
        assert "P001" not in result.output
        assert "P004" in result.output

    def test_max_cell_len(self):
        result = runner.invoke(app, ["view", TEST_CSV, "--max-cell-len", "5"])
        assert result.exit_code == 0
        # "Control" (7 chars) should be truncated to "Contr..."
        assert "Contr..." in result.output
        # "P001" (4 chars) fits within 5, should appear as-is
        assert "P001" in result.output

    def test_no_output_flag(self):
        result = runner.invoke(app, ["view", TEST_CSV, "-o", "csv"])
        assert result.exit_code != 0

    def test_truncation_indicator(self):
        """With no --limit and more than 20 rows, truncation '...' should appear.
        Our test.csv only has 8 rows, so no truncation."""
        result = runner.invoke(app, ["view", TEST_CSV])
        assert result.exit_code == 0
        # 8 rows < 20 default limit, so no truncation
        lines_with_ellipsis = [
            line
            for line in result.output.splitlines()
            if line.strip() == "...   ...   ...   ...   ...   ..."
        ]
        assert len(lines_with_ellipsis) == 0


class TestCat:
    def test_basic_outputs_csv(self):
        result = runner.invoke(app, ["cat", TEST_CSV])
        assert result.exit_code == 0
        # Should output in CSV format (the input format), not a Rich table
        assert (
            "Participant_ID," in result.output
            or "Participant_ID\t" in result.output
            or "P001" in result.output
        )

    def test_output_format_csv(self):
        result = runner.invoke(app, ["cat", TEST_CSV, "-o", "csv"])
        assert result.exit_code == 0
        lines = result.output.strip().splitlines()
        # CSV header
        assert "Participant_ID" in lines[0]
        # Should have header + 8 data rows
        assert len(lines) == 9

    def test_output_format_tsv(self):
        result = runner.invoke(app, ["cat", TEST_CSV, "-o", "tsv"])
        assert result.exit_code == 0
        lines = result.output.strip().splitlines()
        assert "\t" in lines[0]

    def test_no_rich_table(self):
        """cat without -o should NOT produce a Rich formatted table."""
        result = runner.invoke(app, ["cat", TEST_CSV])
        assert result.exit_code == 0
        # Rich tables use box-drawing chars; CSV output won't
        assert "─" not in result.output


class TestSqlOption:
    def test_view_with_sql(self):
        result = runner.invoke(
            app,
            ["view", TEST_CSV, "--sql", "SELECT * FROM t WHERE Status = 'Baseline'"],
        )
        assert result.exit_code == 0
        assert "Baseline" in result.output
        # Should show as a table by default
        assert "Active" not in result.output

    def test_view_with_sql_and_limit(self):
        result = runner.invoke(
            app, ["view", TEST_CSV, "--sql", "SELECT * FROM t", "--limit", "2"]
        )
        assert result.exit_code == 0
        # Should have limited rows
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
            app, ["cat", TEST_CSV, "--jp", "[Participant_ID, Status]", "-o", "jsonl"]
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


class TestStdin:
    """Tests for reading from stdin using '-' as the file path."""

    def test_view_stdin_csv(self):
        result = runner.invoke(app, ["view", "-i", "csv", "-"], input=TEST_CSV_TEXT)
        assert result.exit_code == 0
        assert "P001" in result.output
        assert "Control" in result.output

    def test_view_stdin_requires_format(self):
        result = runner.invoke(app, ["view", "-"], input=TEST_CSV_TEXT)
        assert result.exit_code != 0

    def test_cat_stdin_csv(self):
        result = runner.invoke(app, ["cat", "-i", "csv", "-"], input=TEST_CSV_TEXT)
        assert result.exit_code == 0
        lines = result.output.strip().splitlines()
        assert "Participant_ID" in lines[0]
        assert len(lines) == 9  # header + 8 data rows

    def test_cat_stdin_with_output_format(self):
        result = runner.invoke(
            app, ["cat", "-i", "csv", "-o", "tsv", "-"], input=TEST_CSV_TEXT
        )
        assert result.exit_code == 0
        lines = result.output.strip().splitlines()
        assert "\t" in lines[0]

    def test_schema_stdin_csv(self):
        result = runner.invoke(app, ["schema", "-i", "csv", "-"], input=TEST_CSV_TEXT)
        assert result.exit_code == 0
        assert "Participant_ID" in result.output

    def test_summary_stdin_csv(self):
        result = runner.invoke(app, ["summary", "-i", "csv", "-"], input=TEST_CSV_TEXT)
        assert result.exit_code == 0
        assert "8" in result.output  # 8 rows
        assert "6" in result.output  # 6 columns

    def test_view_stdin_with_sql(self):
        result = runner.invoke(
            app,
            [
                "view",
                "-i",
                "csv",
                "--sql",
                "SELECT * FROM t WHERE Status = 'Baseline'",
                "-",
            ],
            input=TEST_CSV_TEXT,
        )
        assert result.exit_code == 0
        assert "Baseline" in result.output
        assert "Active" not in result.output

    def test_view_stdin_with_limit(self):
        result = runner.invoke(
            app, ["view", "-i", "csv", "--limit", "2", "-"], input=TEST_CSV_TEXT
        )
        assert result.exit_code == 0
        assert "P001" in result.output
        assert "P003" not in result.output


class TestConfigFile:
    """Tests for loading config from ~/.config/tab/config.json."""

    def setup_method(self):
        """Reset global config before each test."""
        config_module.config = Config()

    def test_load_missing_file(self, tmp_path):
        """No-op when the config file does not exist."""
        load_config_file(tmp_path / "nonexistent.json")
        assert config_module.config.az_url_authority_is_account is False
        assert config_module.config.sampling_size_for_schema_inference == 32

    def test_load_valid_config(self, tmp_path):
        """Known keys are applied to the global config."""
        cfg = tmp_path / "config.json"
        cfg.write_text(
            json.dumps(
                {
                    "az_url_authority_is_account": True,
                    "sampling_size_for_schema_inference": 64,
                }
            )
        )
        load_config_file(cfg)
        assert config_module.config.az_url_authority_is_account is True
        assert config_module.config.sampling_size_for_schema_inference == 64

    def test_load_partial_config(self, tmp_path):
        """Only specified keys are changed; others keep defaults."""
        cfg = tmp_path / "config.json"
        cfg.write_text(json.dumps({"sampling_size_for_schema_inference": 128}))
        load_config_file(cfg)
        assert config_module.config.az_url_authority_is_account is False
        assert config_module.config.sampling_size_for_schema_inference == 128

    def test_unknown_keys_ignored(self, tmp_path):
        """Unknown keys are silently ignored (with a warning log)."""
        cfg = tmp_path / "config.json"
        cfg.write_text(
            json.dumps(
                {"unknown_key": "value", "sampling_size_for_schema_inference": 16}
            )
        )
        load_config_file(cfg)
        assert config_module.config.sampling_size_for_schema_inference == 16
        assert not hasattr(config_module.config, "unknown_key")

    def test_invalid_json_raises(self, tmp_path):
        """Non-object JSON raises ValueError."""
        cfg = tmp_path / "config.json"
        cfg.write_text('"just a string"')
        try:
            load_config_file(cfg)
            assert False, "Expected ValueError"
        except ValueError as e:
            assert "JSON object" in str(e)

    def test_cli_flag_overrides_config_file(self, tmp_path):
        """CLI --az-url-authority-is-account overrides the config file value."""
        cfg = tmp_path / "config.json"
        cfg.write_text(json.dumps({"az_url_authority_is_account": False}))
        with patch(
            "tab_cli.cli.load_config_file", side_effect=lambda: load_config_file(cfg)
        ):
            result = runner.invoke(
                app, ["--az-url-authority-is-account", "view", TEST_CSV]
            )
            assert result.exit_code == 0
            assert config_module.config.az_url_authority_is_account is True

    def test_config_file_sets_default(self, tmp_path):
        """Config file value is used when CLI flag is not passed."""
        cfg = tmp_path / "config.json"
        cfg.write_text(json.dumps({"az_url_authority_is_account": True}))
        with patch(
            "tab_cli.cli.load_config_file", side_effect=lambda: load_config_file(cfg)
        ):
            result = runner.invoke(app, ["view", TEST_CSV])
            assert result.exit_code == 0
            assert config_module.config.az_url_authority_is_account is True
