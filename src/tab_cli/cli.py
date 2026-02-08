"""Main CLI entry point using Typer."""

import sys
from typing import Annotated, Optional

from loguru import logger
import polars as pl
import typer
from rich.console import Console
from rich.logging import RichHandler

from tab_cli import config
from tab_cli.handlers import TableWriter, infer_reader, infer_writer
from tab_cli.handlers.cli_table import CliTableFormatter

app = typer.Typer(
    help="A CLI tool for viewing and manipulating tabular data.",
    no_args_is_help=True,
)


@app.callback()
def main_callback(
    az_url_authority_is_account: Annotated[
        bool,
        typer.Option(
            "--az-url-authority-is-account",
            help="Interpret az:// URL authority as storage account name instead of container name",
        ),
    ] = False,
    log_level: Annotated[
        str,
        typer.Option("--log-level", help="Log level from {DEBUG, INFO, WARNING, ERROR, CRITICAL}"),
    ] = "INFO",
) -> None:
    """Global options for tab_cli CLI."""
    config.config.az_url_authority_is_account = az_url_authority_is_account
    logger.remove()
    logger.add(
        RichHandler(
            rich_tracebacks=True,
            tracebacks_show_locals=True,
            markup=True,
        ),
        format="{message}",
        level=log_level.upper(),
    )


def _apply_limit(
    lf: pl.LazyFrame,
    limit: int | None,
    skip: int,
    default_limit: int | None = None,
) -> tuple[pl.LazyFrame, bool]:
    """Apply skip/limit to a LazyFrame, optionally detecting truncation.

    If limit is None and default_limit is set, caps at default_limit rows
    and returns whether the data was truncated.
    """
    if limit is None and default_limit is not None:
        lf = lf.slice(skip, length=default_limit + 1)
        df = lf.collect()
        truncated = len(df) > default_limit
        if truncated:
            df = df.head(default_limit)
        return df.lazy(), truncated
    else:
        if skip > 0 or limit is not None:
            lf = lf.slice(skip, length=limit)
        return lf, False


@app.command()
def view(
    path: Annotated[str, typer.Argument(help="Path to the data file or directory")],
    limit: Annotated[Optional[int], typer.Option("--limit", help="Maximum number of rows to display")] = None,
    skip: Annotated[int, typer.Option("--skip", help="Number of rows to skip")] = 0,
    input: Annotated[Optional[str], typer.Option("-i", "--input-format", help="Input format, auto-detected from extension if omitted")] = None,
    max_cell_len: Annotated[Optional[int], typer.Option("--max-cell-len", help="Truncate cell contents longer than this")] = None,
) -> None:
    """View tabular data as a formatted table."""
    reader = infer_reader(path, format=input)
    lf = reader.read(path)
    lf, truncated = _apply_limit(lf, limit=limit, skip=skip, default_limit=20 if limit is None else None)
    writer = CliTableFormatter(truncated=truncated, max_cell_len=max_cell_len)
    for chunk in writer.write(lf):
        sys.stdout.buffer.write(chunk)

@app.command()
def schema(
    path: Annotated[str, typer.Argument(help="Path to the data file or directory")],
    input: Annotated[Optional[str], typer.Option("-i", "--input-format", help="Input format")] = None,
) -> None:
    """Display the schema of a tabular data file."""
    reader = infer_reader(path, format=input)
    table_schema = reader.schema(path)
    console = Console(force_terminal=True)
    console.print(table_schema)


@app.command()
def sql(
    query: Annotated[str, typer.Argument(help="SQL query to execute (table is available as 't')")],
    path: Annotated[str, typer.Argument(help="Path to the data file or directory")],
    limit: Annotated[Optional[int], typer.Option("--limit", help="Maximum number of rows to display")] = None,
    skip: Annotated[int, typer.Option("--skip", help="Number of rows to skip")] = 0,
    input: Annotated[Optional[str], typer.Option("-i", "--input-format", help="Input format")] = None,
    output: Annotated[Optional[str], typer.Option("-o", "--output-format", help="Output format")] = None,
) -> None:
    """Run a SQL query on tabular data. The table is available as 't'."""
    reader = infer_reader(path, format=input)
    lf = reader.read(path)
    ctx = pl.SQLContext(t=lf, eager=False)
    result_lf = ctx.execute(query)
    show_truncation = limit is None and output is None
    result_lf, truncated = _apply_limit(result_lf, limit=limit, skip=skip, default_limit=20 if show_truncation else None)
    writer = infer_writer(output, truncated=truncated)
    for chunk in writer.write(result_lf):
        sys.stdout.buffer.write(chunk)


@app.command()
def summary(
    path: Annotated[str, typer.Argument(help="Path to the data file or directory")],
    input: Annotated[Optional[str], typer.Option("-i", "--input-format", help="Input format")] = None,
) -> None:
    """Display summary information about a tabular data file."""
    handler = infer_reader(path, format=input)
    table_summary = handler.summary(path)
    console = Console(force_terminal=True)
    console.print(table_summary)


@app.command()
def convert(
    src: Annotated[str, typer.Argument(help="Path to the source file or directory")],
    dst: Annotated[str, typer.Argument(help="Path to the destination file or directory")],
    input: Annotated[Optional[str], typer.Option("-i", "--input-format", help="Input format")] = None,
    output: Annotated[Optional[str], typer.Option("-o", "--output-format", help="Output format")] = None,
    num_partitions: Annotated[Optional[int], typer.Option("-n", "--num-partitions", help="Number of output partitions")] = None,
) -> None:
    """Convert tabular data from one format to another."""
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


@app.command()
def cat(
    paths: Annotated[list[str], typer.Argument(help="Paths to the data files or directories")],
    input: Annotated[Optional[str], typer.Option("-i", "--input-format", help="Input format")] = None,
    output: Annotated[Optional[str], typer.Option("-o", "--output-format", help="Output format")] = None,
) -> None:
    """Concatenate tabular data from multiple files, or just print a single file."""
    reader = infer_reader(paths[0], format=input)
    files = [reader.read(path) for path in paths]
    lf = pl.concat(files, how="vertical")
    if output is not None:
        writer = infer_writer(format=output)
    else:
        writer = infer_writer(format=reader.format.extension())
        assert isinstance(writer, TableWriter)
    for chunk in writer.write(lf):
        sys.stdout.buffer.write(chunk)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
