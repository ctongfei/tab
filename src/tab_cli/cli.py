"""Main CLI entry point using Typer."""

import sys
from typing import Annotated, Optional, TypeAlias

from loguru import logger
import polars as pl
import typer
from rich.console import Console
from rich.logging import RichHandler

from tab_cli import config
from tab_cli.handlers import TableWriter, infer_reader, infer_writer

# Reusable type aliases for common CLI options
PathArg: TypeAlias = Annotated[str, typer.Argument(help="Path to the data file or directory")]
PathsArg: TypeAlias = Annotated[list[str], typer.Argument(help="Paths to the data files or directories")]
SrcArg: TypeAlias = Annotated[str, typer.Argument(help="Path to the source file or directory")]
DstArg: TypeAlias = Annotated[str, typer.Argument(help="Path to the destination file or directory")]
InputOpt: TypeAlias = Annotated[Optional[str], typer.Option("-i", "--input-format", help="Input format, auto-detected from extension if omitted")]
OutputOpt: TypeAlias = Annotated[Optional[str], typer.Option("-o", "--output-format", help="Output format")]
SqlOpt: TypeAlias = Annotated[Optional[str], typer.Option("--sql", help="SQL query to apply (table is available as 't')")]
LimitOpt: TypeAlias = Annotated[Optional[int], typer.Option("--limit", help="Maximum number of rows to display")]
SkipOpt: TypeAlias = Annotated[int, typer.Option("--skip", help="Number of rows to skip")]
MaxCellLenOpt: TypeAlias = Annotated[Optional[int], typer.Option("--max-cell-len", help="Truncate cell contents longer than this")]
TableSvgOpt: TypeAlias = Annotated[bool, typer.Option("--table-svg", help="Output table as SVG")]
NumPartitionsOpt: TypeAlias = Annotated[Optional[int], typer.Option("-n", "--num-partitions", help="Number of output partitions")]

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


def _apply_sql(lf: pl.LazyFrame, sql: str | None) -> pl.LazyFrame:
    """Apply an optional SQL query to a LazyFrame. The table is available as 't'."""
    if sql is not None:
        ctx = pl.SQLContext(t=lf, eager=False)
        return ctx.execute(sql)
    return lf


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
    path: PathArg,
    limit: LimitOpt = None,
    skip: SkipOpt = 0,
    input: InputOpt = None,
    sql: SqlOpt = None,
    max_cell_len: MaxCellLenOpt = None,
    table_svg: TableSvgOpt = False,
) -> None:
    """View tabular data as a formatted table."""
    reader = infer_reader(path, format=input)
    lf = reader.read(path)
    lf = _apply_sql(lf, sql)
    lf, truncated = _apply_limit(lf, limit=limit, skip=skip, default_limit=20 if limit is None else None)
    writer = infer_writer("table-svg" if table_svg else None, truncated=truncated, max_cell_len=max_cell_len)
    for chunk in writer.write(lf):
        sys.stdout.buffer.write(chunk)

@app.command()
def schema(
    path: PathArg,
    input: InputOpt = None,
) -> None:
    """Display the schema of a tabular data file."""
    reader = infer_reader(path, format=input)
    table_schema = reader.schema(path)
    console = Console(force_terminal=True)
    console.print(table_schema)


@app.command()
def summary(
    path: PathArg,
    input: InputOpt = None,
) -> None:
    """Display summary information about a tabular data file."""
    handler = infer_reader(path, format=input)
    table_summary = handler.summary(path)
    console = Console(force_terminal=True)
    console.print(table_summary)


@app.command()
def convert(
    src: SrcArg,
    dst: DstArg,
    input: InputOpt = None,
    output: OutputOpt = None,
    sql: SqlOpt = None,
    num_partitions: NumPartitionsOpt = None,
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
    lf = _apply_sql(lf, sql)
    writer.write_to_path(lf, dst, partitions=num_partitions)


@app.command()
def cat(
    paths: PathsArg,
    input: InputOpt = None,
    output: OutputOpt = None,
    sql: SqlOpt = None,
) -> None:
    """Concatenate tabular data from multiple files, or just print a single file."""
    reader = infer_reader(paths[0], format=input)
    files = [reader.read(path) for path in paths]
    lf = pl.concat(files, how="vertical")
    lf = _apply_sql(lf, sql)
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
