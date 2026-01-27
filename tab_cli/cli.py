"""Main CLI entry point using Fire."""

import sys

import fire
import polars as pl
from rich.console import Console

from tab_cli.handlers import infer_reader, infer_writer, TableWriter


class Tab:

    def _output(
        self,
        lf: pl.LazyFrame,
        limit: int | None,
        skip: int,
        output: str | None,
    ) -> None:
        show_truncation = limit is None and output is None
        actual_limit = 10 if show_truncation else limit

        if show_truncation:
            lf = lf.slice(skip, length=actual_limit + 1)
            df = lf.collect()
            truncated = len(df) > actual_limit
            if truncated:
                df = df.head(actual_limit)
            lf = df.lazy()
        else:
            if skip > 0 or actual_limit is not None:
                lf = lf.slice(skip, length=actual_limit)
            truncated = False

        writer = infer_writer(output, truncated=show_truncation and truncated)

        for chunk in writer.write(lf):
            sys.stdout.buffer.write(chunk)

    def view(
        self,
        path: str,
        limit: int | None = None,
        skip: int = 0,
        output: str | None = None,
        input: str | None = None,
    ) -> None:
        """View tabular data from a file.

        Args:
            path: Path to the data file.
            limit: Maximum number of rows to display (default: 10).
            skip: Number of rows to skip from the beginning (default: 0).
            output: Output format ('jsonl', 'csv', 'tsv', 'parquet'). Default is a rich table.
            input: Input format ('parquet', 'csv', 'tsv'). Default is inferred from extension.
        """
        reader = infer_reader(path, format=input)
        lf = reader.read(path)
        self._output(lf, limit=limit, skip=skip, output=output)

    def schema(self, path: str, input: str | None = None) -> None:
        """Display the schema of a tabular data file.

        Args:
            path: Path to the data file.
            input: Input format ('parquet', 'csv', 'tsv'). Default is inferred from extension.
        """
        reader = infer_reader(path, format=input)
        table_schema = reader.schema(path)
        console = Console(force_terminal=True)
        console.print(table_schema)

    def sql(
        self,
        query: str,
        path: str,
        limit: int | None = None,
        skip: int = 0,
        output: str | None = None,
        input: str | None = None,
    ) -> None:
        """Run a SQL query on tabular data. The table is available as `t`.

        Args:
            query: SQL query to execute. Reference the data as table `t`.
            path: Path to the data file.
            limit: Maximum number of rows to display (default: 10).
            skip: Number of rows to skip from the beginning (default: 0).
            output: Output format ('jsonl', 'csv', 'tsv', 'parquet'). Default is a rich table.
            input: Input format ('parquet', 'csv', 'tsv'). Default is inferred from extension.
        """
        reader = infer_reader(path, format=input)
        lf = reader.read(path)
        ctx = pl.SQLContext(t=lf, eager=False)
        result_lf = ctx.execute(query)
        self._output(result_lf, limit=limit, skip=skip, output=output)

    def summary(self, path: str, input: str | None = None) -> None:
        """Display summary information about a tabular data file.

        Args:
            path: Path to the data file.
            input: Input format ('parquet', 'csv', 'tsv'). Default is inferred from extension.
        """
        handler = infer_reader(path, format=input)
        table_summary = handler.summary(path)
        console = Console(force_terminal=True)
        console.print(table_summary)

    def convert(
        self,
        src: str,
        dst: str,
        input: str | None = None,
        output: str | None = None,
        num_partitions: int | None = None,
    ) -> None:
        """Convert tabular data from one format to another.

        Args:
            src: Path to the source data file.
            dst: Path to the destination file or directory.
            input: Input format ('parquet', 'csv', 'tsv'). Default is inferred from src extension.
            output: Output format ('parquet', 'csv', 'tsv'). Default is same as input format.
            num_partitions: Number of output partitions. If not specified, writes to a single file.
        """
        reader = infer_reader(src, format=input)
        # Determine output format: use -o if specified, else inherit from input
        if output is not None:
            writer = infer_writer(format=output)
        elif input is not None:
            writer = infer_writer(format=input)
        else:
            writer = reader
            assert isinstance(writer, TableWriter)
        lf = reader.read(src)
        writer.write_to_path(lf, dst, partitions=num_partitions)


def main():
    fire.Fire(Tab)


if __name__ == "__main__":
    main()
