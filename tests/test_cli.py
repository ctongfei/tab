"""Tests for the tab CLI commands."""

import os

from typer.testing import CliRunner

from tab_cli.cli import app

runner = CliRunner()
TEST_CSV = os.path.join(os.path.dirname(__file__), "assets", "test.csv")


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
        lines_with_ellipsis = [l for l in result.output.splitlines() if l.strip() == "...   ...   ...   ...   ...   ..."]
        assert len(lines_with_ellipsis) == 0


class TestCat:
    def test_basic_outputs_csv(self):
        result = runner.invoke(app, ["cat", TEST_CSV])
        assert result.exit_code == 0
        # Should output in CSV format (the input format), not a Rich table
        assert "Participant_ID," in result.output or "Participant_ID\t" in result.output or "P001" in result.output

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


class TestSql:
    def test_basic_table_output(self):
        result = runner.invoke(app, ["sql", "SELECT * FROM t WHERE Status = 'Baseline'", TEST_CSV])
        assert result.exit_code == 0
        assert "Baseline" in result.output
        # Should show as a table by default (no -o)
        assert "Active" not in result.output

    def test_with_output_format(self):
        result = runner.invoke(app, ["sql", "SELECT Participant_ID, Status FROM t", TEST_CSV, "-o", "csv"])
        assert result.exit_code == 0
        lines = result.output.strip().splitlines()
        assert "Participant_ID" in lines[0]
        assert "Status" in lines[0]

    def test_limit(self):
        result = runner.invoke(app, ["sql", "SELECT * FROM t", TEST_CSV, "--limit", "2"])
        assert result.exit_code == 0
        # Should have limited rows
        count = sum(1 for line in result.output.splitlines() if "P00" in line)
        assert count <= 2
