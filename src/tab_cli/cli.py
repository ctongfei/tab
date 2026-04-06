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
MaxCellLengthOpt: TypeAlias = Annotated[
    Optional[int],
    typer.Option("--max-cell-length", help="Truncate cell contents longer than this"),
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

DEFAULT_VIEW_TRUNCATION_PROBE_ROWS = 1
VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


def _normalize_log_level(log_level: str) -> str:
    normalized = log_level.upper()
    if normalized not in VALID_LOG_LEVELS:
        valid_levels = ", ".join(sorted(VALID_LOG_LEVELS))
        raise typer.BadParameter(f"Invalid log level '{log_level}'. Expected one of: {valid_levels}")
    return normalized


def _configure_logger(level: str) -> None:
    logger.remove()
    logger.add(
        RichHandler(
            rich_tracebacks=True,
            tracebacks_show_locals=True,
            markup=True,
        ),
        format="{message}",
        level=level,
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
        str | None,
        typer.Option(
            "--log-level",
            help="Log level from {DEBUG, INFO, WARNING, ERROR, CRITICAL}; defaults to config when omitted",
        ),
    ] = None,
) -> None:
    """Global options for tab_cli CLI."""
    logger.remove()
    loaded_config = load_config_file()
    effective_config = loaded_config

    effective_config.log_level = (
        _normalize_log_level(log_level)
        if log_level is not None
        else _normalize_log_level(effective_config.log_level)
    )

    if az_url_authority_is_account:
        effective_config.az_url_authority_is_account = az_url_authority_is_account

    config.config = effective_config
    _configure_logger(config.config.log_level)


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
    logger.debug(
        f"Inferring JMESPath output schema from {Config.sampling_size_for_schema_inference} sampled row(s)"
    )
    if sample_df.is_empty():
        logger.debug("JMESPath schema inference sample was empty; returning empty LazyFrame")
        return pl.DataFrame().lazy()

    transformed_sample, result_mode = _transform_jmespath_batch(sample_df, compiled)
    output_schema = transformed_sample.schema
    expected_columns = tuple(transformed_sample.columns)
    logger.debug(
        f"Inferred JMESPath result mode '{result_mode}' with columns {expected_columns}"
    )

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
        logger.debug(
            f"Applying inferred default row limit {default_limit} with skip {skip}"
        )
        lf = lf.slice(skip, length=default_limit + 1)
        df = lf.collect()
        truncated = len(df) > default_limit
        if truncated:
            df = df.head(default_limit)
            logger.debug("Detected truncated preview after applying inferred default row limit")
        return df.lazy(), truncated
    if skip > 0 or limit is not None:
        lf = lf.slice(skip, length=limit)
    return lf, False


def _read_source(path: str, input_format: str | None) -> tuple[pl.LazyFrame, str | None]:
    """Read a source path and return its LazyFrame and inferred format."""
    if is_stdin(path):
        logger.debug(
            "Using stdin source with explicit format "
            f"'{input_format.lower() if input_format is not None else None}'"
        )
        return (
            read_stdin(format=input_format),
            input_format.lower() if input_format is not None else None,
        )

    reader = infer_reader(path, format=input_format)
    logger.debug(
        f"Read source '{path}' using inferred format '{reader.format.extension()}'"
    )
    return reader.read(path), reader.format.extension()


def _prepare_view_frame(
    path: str,
    input_format: str | None,
    sql: str | None,
    jmespath_expr: str | None,
    limit: int | None,
    skip: int,
) -> tuple[pl.LazyFrame, bool]:
    """Prepare the LazyFrame used by `tab view` and report truncation."""
    default_view_rows = config.config.default_num_view_rows

    if is_stdin(path):
        logger.debug("Preparing view for stdin input")
        lf = read_stdin(format=input_format)
        lf = _apply_query(lf, sql=sql, jmespath_expr=jmespath_expr)
        return _apply_limit(
            lf,
            limit=limit,
            skip=skip,
            default_limit=default_view_rows if limit is None else None,
        )

    reader = infer_reader(path, format=input_format)
    if sql is None and jmespath_expr is None:
        preview_limit = (
            limit
            if limit is not None
            else default_view_rows + DEFAULT_VIEW_TRUNCATION_PROBE_ROWS
        )
        logger.debug(
            f"Using preview read for '{path}' with inferred preview limit "
            f"{preview_limit} and skip {skip}"
        )
        lf = reader.read_preview(path, limit=preview_limit, offset=skip)
        if limit is not None:
            return lf, False

        df = lf.collect()
        truncated = len(df) > default_view_rows
        if truncated:
            df = df.head(default_view_rows)
        return df.lazy(), truncated

    logger.debug(f"Using full read for '{path}' because a query transform was provided")
    lf = reader.read(path)
    lf = _apply_query(lf, sql=sql, jmespath_expr=jmespath_expr)
    return _apply_limit(
        lf,
        limit=limit,
        skip=skip,
        default_limit=default_view_rows if limit is None else None,
    )


def _resolve_cat_output_format(
    paths: list[str],
    input_format: str | None,
) -> tuple[list[pl.LazyFrame], str | None]:
    """Read all inputs for `tab cat` and validate format consistency."""
    files: list[pl.LazyFrame] = []
    resolved_format = input_format.lower() if input_format is not None else None
    if resolved_format is not None:
        logger.debug(f"Using explicit shared input format '{resolved_format}' for `tab cat`")

    for path in paths:
        lf, current_format = _read_source(path, input_format)
        if current_format is not None:
            if resolved_format is None:
                resolved_format = current_format
                logger.debug(
                    f"Inferred shared `tab cat` format '{resolved_format}' from '{path}'"
                )
            elif current_format != resolved_format:
                raise ValueError(
                    "All inputs to `tab cat` must use the same format unless -i/--input-format is provided"
                )
        files.append(lf)

    return files, resolved_format


def _requires_explicit_output_for_database_input(
    paths: list[str],
    input_format: str | None,
) -> str | None:
    if input_format is not None and input_format.lower() in {"sqlite", "duckdb"}:
        return input_format.lower()

    for path in paths:
        lowered_path = path.rsplit("#", 1)[0].lower()
        if lowered_path.endswith((".db", ".sqlite", ".sqlite3")):
            return "sqlite"
        if lowered_path.endswith((".duckdb", ".ddb")):
            return "duckdb"

    return None


@app.command()
def view(
    path: PathArg,
    limit: LimitOpt = None,
    skip: SkipOpt = 0,
    input: InputOpt = None,
    sql: SqlOpt = None,
    jmespath_expr: JmespathOpt = None,
    max_cell_len: MaxCellLengthOpt = None,
    table_svg: TableSvgOpt = False,
) -> None:
    """View tabular data as a formatted table."""
    effective_max_cell_len = (
        max_cell_len if max_cell_len is not None else config.config.max_cell_length
    )
    if max_cell_len is None and effective_max_cell_len is not None:
        logger.debug(
            f"Inferred max_cell_len={effective_max_cell_len} for `tab view` from config"
        )
    lf, truncated = _prepare_view_frame(
        path,
        input_format=input,
        sql=sql,
        jmespath_expr=jmespath_expr,
        limit=limit,
        skip=skip,
    )
    writer = infer_writer(
        "table-svg" if table_svg else None,
        truncated=truncated,
        max_cell_len=effective_max_cell_len,
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
    console = Console()
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
    console = Console()
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
            logger.debug(
                f"Inferred convert output format '{input.lower()}' from stdin input format"
            )
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
            logger.debug(
                f"Inferred convert output format '{input.lower()}' from explicit input format override"
            )
            writer = infer_writer(format=input)
        else:
            writer = reader
            logger.debug(
                f"Inferred convert output format '{reader.format.extension()}' from source '{src}'"
            )
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
    if output is None:
        database_format = _requires_explicit_output_for_database_input(paths, input)
        if database_format is not None:
            raise ValueError(
                f"Output format (-o/--output-format) is required when reading from {database_format.capitalize()} input"
            )

    files, resolved_format = _resolve_cat_output_format(paths, input)
    lf = pl.concat(files, how="vertical")
    lf = _apply_query(lf, sql=sql, jmespath_expr=jmespath_expr)
    if output is not None:
        writer = infer_writer(format=output)
    elif resolved_format in {"sqlite", "duckdb"}:
        raise ValueError(
            f"Output format (-o/--output-format) is required when reading from {resolved_format.capitalize()} input"
        )
    elif resolved_format is not None:
        logger.debug(f"Inferred `tab cat` output format '{resolved_format}' from input sources")
        writer = infer_writer(format=resolved_format)
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
