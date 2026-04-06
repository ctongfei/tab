import os

from typer.testing import CliRunner


runner = CliRunner()
TEST_CSV = os.path.join(os.path.dirname(__file__), "assets", "test.csv")

with open(TEST_CSV, "rb") as test_csv_file:
    TEST_CSV_BYTES = test_csv_file.read()

TEST_CSV_TEXT = TEST_CSV_BYTES.decode("utf-8")
