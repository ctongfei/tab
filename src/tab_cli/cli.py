"""Main CLI entry point using Typer."""

import sys
from typing import Annotated, Any, Optional, TypeAlias

import jmespath
from jmespath.parser import ParsedResult
from loguru import logger
import polars as pl
import typer
from rich.console import Console
from rich.logging import RichHandler

from tab_cli import config
from tab_cli.config import Config, load_config_file
from tab_cli.handlers import (
    TableWriter,
    infer_reader,
    infer_writer,
    is_stdin,
    read_stdin,
)
from tab_cli.handlers.base import TableSchema, TableSummary

# Reusable type aliases for common CLI options
PathArg: TypeAlias = Annotated[
    str, typer.Argument(help="Path to the data file or directory")
]
PathsArg: TypeAlias = Annotated[
    list[str], typer.Argument(help="Paths to the data files or directories")
]
SrcArg: TypeAlias = Annotated[
    str, typer.Argument(help="Path to the source file or directory")
]
DstArg: TypeAlias = Annotated[
    str, typer.Argument(help="Path to the destination file or directory")
]
InputOpt: TypeAlias = Annotated[
    Optional[str],
    typer.Option(
        "-i",
        "--input-format",
        help="Input format, auto-detected from extension if omitted",
    ),
]
OutputOpt: TypeAlias = Annotated[
    Optional[str], typer.Option("-o", "--output-format", help="Output format")
]
SqlOpt: TypeAlias = Annotated[
    Optional[str],
    typer.Option("--sql", help="SQL query to apply (table is available as 't')"),
]
JmespathOpt: TypeAlias = Annotated[
    Optional[str],
    typer.Option(
        "--jmespath", "--jp", help="JMESPath expression to apply to each row as JSON"
    ),
]
LimitOpt: TypeAlias = Annotated[
    Optional[int], typer.Option("--limit", help="Maximum number of rows to display")
]
SkipOpt: TypeAlias = Annotated[
    int, typer.Option("--skip", help="Number of rows to skip")
]
MaxCellLenOpt: TypeAlias = Annotated[
    Optional[int],
    typer.Option("--max-cell-len", help="Truncate cell contents longer than this"),
]
TableSvgOpt: TypeAlias = Annotated[
    bool, typer.Option("--table-svg", help="Output table as SVG")
]
NumPartitionsOpt: TypeAlias = Annotated[
    Optional[int],
    typer.Option("-n", "--num-partitions", help="Number of output partitions"),
]

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
        typer.Option(
            "--log-level", help="Log level from {DEBUG, INFO, WARNING, ERROR, CRITICAL}"
        ),
    ] = "INFO",
) -> None:
    """Global options for tab_cli CLI."""
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
    load_config_file()
    # CLI flags override config file values
    if az_url_authority_is_account:
        config.config.az_url_authority_is_account = az_url_authority_is_account


def _apply_sql(lf: pl.LazyFrame, sql: str | None) -> pl.LazyFrame:
    """Apply an optional SQL query to a LazyFrame. The table is available as 't'."""
    if sql is not None:
        ctx = pl.SQLContext(t=lf, eager=False)
        return ctx.execute(sql)
    return lf


def _transform_jmespath_batch(
    df: pl.DataFrame,
    expression: ParsedResult,
    result_mode: str | None = None,
    expected_columns: tuple[str, ...] | None = None,
    output_schema: dict[str, pl.DataType] | None = None,
) -> tuple[pl.DataFrame, str | None]:
    """Transform a batch with JMESPath and normalize it to a rectangular DataFrame."""
    rows: list[dict[str, Any]] = []
    mode = result_mode

    for row in df.iter_rows(named=True):
        result = expression.search(row)

        if isinstance(result, dict):
            if mode is None:
                mode = "object"
            elif mode != "object":
                raise ValueError(
                    "JMESPath query must return a consistent shape across rows"
                )

            if expected_columns is not None:
                extra_columns = set(result) - set(expected_columns)
                if extra_columns:
                    extras = ", ".join(sorted(extra_columns))
                    raise ValueError(
                        f"JMESPath query produced unexpected columns: {extras}"
                    )
                normalized_row = {
                    column: result.get(column) for column in expected_columns
                }
            else:
                normalized_row = result
        else:
            if mode is None:
                mode = "value"
            elif mode != "value":
                raise ValueError(
                    "JMESPath query must return a consistent shape across rows"
                )
            normalized_row = {"value": result}

        rows.append(normalized_row)

    if output_schema is not None:
        return pl.from_dicts(rows, schema=output_schema, strict=False), mode
    return pl.from_dicts(rows), mode


def _apply_jmespath(lf: pl.LazyFrame, expression: str) -> pl.LazyFrame:
    """Apply a JMESPath expression to each row of a LazyFrame."""

    compiled = jmespath.compile(expression)
    sample_df = lf.slice(0, Config.sampling_size_for_schema_inference).collect()
    if sample_df.is_empty():
        return pl.DataFrame().lazy()

    transformed_sample, result_mode = _transform_jmespath_batch(sample_df, compiled)
    output_schema = transformed_sample.schema
    expected_columns = tuple(transformed_sample.columns)

    return lf.map_batches(
        lambda batch: _transform_jmespath_batch(
            batch,
            compiled,
            result_mode=result_mode,
            expected_columns=expected_columns,
            output_schema=output_schema,
        )[0],
        schema=output_schema,
        streamable=True,
    )


def _apply_query(
    lf: pl.LazyFrame, sql: str | None, jmespath_expr: str | None
) -> pl.LazyFrame:
    """Apply exactly zero or one supported query transform to a LazyFrame."""
    if sql is not None and jmespath_expr is not None:
        raise ValueError(
            "At most one query may be provided: use either --sql or --jmespath/--jp"
        )
    if sql is not None:
        return _apply_sql(lf, sql)
    if jmespath_expr is not None:
        return _apply_jmespath(lf, jmespath_expr)
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
    jmespath_expr: JmespathOpt = None,
    max_cell_len: MaxCellLenOpt = None,
    table_svg: TableSvgOpt = False,
) -> None:
    """View tabular data as a formatted table."""
    if is_stdin(path):
        lf = read_stdin(format=input)
    else:
        reader = infer_reader(path, format=input)
        lf = reader.read(path)
    lf = _apply_query(lf, sql=sql, jmespath_expr=jmespath_expr)
    lf, truncated = _apply_limit(
        lf, limit=limit, skip=skip, default_limit=20 if limit is None else None
    )
    writer = infer_writer(
        "table-svg" if table_svg else None,
        truncated=truncated,
        max_cell_len=max_cell_len,
    )
    for chunk in writer.write(lf):
        sys.stdout.buffer.write(chunk)


@app.command()
def schema(
    path: PathArg,
    input: InputOpt = None,
) -> None:
    """Display the schema of a tabular data file."""
    if is_stdin(path):
        lf = read_stdin(format=input)
        columns = list(lf.collect_schema().items())
        table_schema = TableSchema(columns=columns)
    else:
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
    if is_stdin(path):
        lf = read_stdin(format=input)
        df = lf.collect()
        table_summary = TableSummary(
            file_size=0,
            num_rows=len(df),
            num_columns=len(df.columns),
        )
    else:
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
    jmespath_expr: JmespathOpt = None,
    num_partitions: NumPartitionsOpt = None,
) -> None:
    """Convert tabular data from one format to another."""
    if is_stdin(src):
        lf = read_stdin(format=input)
        lf = _apply_query(lf, sql=sql, jmespath_expr=jmespath_expr)
        if output is not None:
            writer = infer_writer(format=output)
        elif input is not None:
            writer = infer_writer(format=input)
        else:
            raise ValueError(
                "Output format (-o/--output-format) is required when reading from stdin (-)"
            )
        assert isinstance(writer, TableWriter)
        writer.write_to_path(lf, dst, partitions=num_partitions)
    else:
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
        lf = _apply_query(lf, sql=sql, jmespath_expr=jmespath_expr)
        writer.write_to_path(lf, dst, partitions=num_partitions)


@app.command()
def cat(
    paths: PathsArg,
    input: InputOpt = None,
    output: OutputOpt = None,
    sql: SqlOpt = None,
    jmespath_expr: JmespathOpt = None,
) -> None:
    """Concatenate tabular data from multiple files, or just print a single file."""
    files: list[pl.LazyFrame] = []
    reader = None
    for path in paths:
        if is_stdin(path):
            files.append(read_stdin(format=input))
        else:
            if reader is None:
                reader = infer_reader(path, format=input)
            files.append(reader.read(path))
    lf = pl.concat(files, how="vertical")
    lf = _apply_query(lf, sql=sql, jmespath_expr=jmespath_expr)
    if output is not None:
        writer = infer_writer(format=output)
    elif reader is not None:
        writer = infer_writer(format=reader.format.extension())
        assert isinstance(writer, TableWriter)
    elif input is not None:
        writer = infer_writer(format=input)
        assert isinstance(writer, TableWriter)
    else:
        raise ValueError(
            "Output format (-o/--output-format) or input format (-i/--input-format) is required when reading from stdin (-)"
        )
    for chunk in writer.write(lf):
        sys.stdout.buffer.write(chunk)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
