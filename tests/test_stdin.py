from tab_cli.cli import app

from tests.conftest import TEST_CSV_TEXT, runner


class TestStdin:
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
        assert len(lines) == 9

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
        assert "8" in result.output
        assert "6" in result.output

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
