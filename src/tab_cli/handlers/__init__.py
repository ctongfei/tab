"""Handler registration and inference."""

import os
import sys

import polars as pl

from tab_cli.formats import AvroFormat, CsvFormat, JsonlFormat, ParquetFormat
from tab_cli.formats.base import FormatHandler
from tab_cli.handlers.base import FormatWriter, TableReader, TableWriter
from tab_cli.handlers.cli_table import CliTableFormatter
from tab_cli.storage import get_backend

# Format handlers
_FORMAT_MAP: dict[str, FormatHandler] = {
    "csv": CsvFormat(","),
    "tsv": CsvFormat("\t"),
    "parquet": ParquetFormat(),
    "jsonl": JsonlFormat(),
    "avro": AvroFormat(),
}


STDIN_PATH = "-"


def is_stdin(path: str) -> bool:
    """Check if the path indicates stdin."""
    return path == STDIN_PATH


def _get_format(format: str | None) -> FormatHandler:
    """Resolve a format name to a FormatHandler, raising if unknown."""
    if format is None:
        raise ValueError("Input format (-i/--input-format) is required when reading from stdin (-)")
    fmt = _FORMAT_MAP.get(format.lower())
    if fmt is None:
        raise ValueError(f"Unknown format: {format}. Supported: {', '.join(_FORMAT_MAP)}")
    return fmt


def read_stdin(format: str | None = None) -> pl.LazyFrame:
    """Read tabular data from stdin.

    Args:
        format: The input format (e.g. 'csv', 'tsv', 'jsonl', 'parquet', 'avro').
            Required since format cannot be inferred from stdin.

    Returns:
        A LazyFrame with the data read from stdin.
    """
    fmt = _get_format(format)
    return fmt.read_stream(sys.stdin.buffer).lazy()


def _get_extension(path: str) -> str:
    """Extract file extension from a path or URL."""
    # Handle URIs by getting the path component
    if "://" in path:
        path = path.split("://", 1)[1]
    # Get extension from basename
    basename = os.path.basename(path.rstrip("/"))
    return os.path.splitext(basename)[1][1:].lower()


def _is_data_file(path: str) -> bool:
    """Check if a file is a data file (not metadata/marker file)."""
    basename = os.path.basename(path.rstrip("/"))
    # Skip hidden files (e.g., .crc files) or Spark/Hadoop marker and metadata files
    return not (basename.startswith(".") or basename.startswith("_"))


def infer_reader(path: str, format: str | None = None) -> TableReader:
    """Infer the reader for a file.

    Args:
        path: Path to the file or directory (local or cloud URL).
        format: Explicit format override. If None, inferred from extension.

    Returns:
        TableReader configured for the format and storage backend.
    """
    backend = get_backend(path)

    if format is not None:
        fmt = _FORMAT_MAP.get(format.lower())
        if fmt is None:
            raise ValueError(f"Unknown format: {format}. Supported: {', '.join(_FORMAT_MAP)}")
        return TableReader(backend, fmt)

    # Infer format from path
    if backend.is_directory(path):
        # Get extension from first data file in directory (skip metadata files)
        for file_info in backend.list_files(path, ""):
            if not _is_data_file(file_info.url):
                continue
            extension = _get_extension(file_info.url)
            if extension:
                break
        else:
            raise ValueError(f"No data files found in directory: {path}")
    else:
        extension = _get_extension(path)

    fmt = _FORMAT_MAP.get(extension)
    if fmt is None:
        raise ValueError(f"Unknown extension: {extension}. Supported: {', '.join(_FORMAT_MAP)}")

    return TableReader(backend, fmt)


def infer_writer(format: str | None = None, truncated: bool = False, max_cell_len: int | None = None) -> TableWriter:
    """Infer the writer for a format.

    Args:
        format: Output format. If None, returns CLI table formatter.
        truncated: Whether the output is truncated (for CLI display).
        max_cell_len: Maximum cell content length for CLI table display.

    Returns:
        TableWriter for the format.
    """
    if format is None:
        return CliTableFormatter(truncated=truncated, max_cell_len=max_cell_len)
    if format == "table-svg":
        return CliTableFormatter(truncated=truncated, svg_capture=True, max_cell_len=max_cell_len)

    fmt = _FORMAT_MAP.get(format.lower())
    if fmt is None:
        raise ValueError(f"Unknown format: {format}. Supported: {', '.join(_FORMAT_MAP)}")

    return FormatWriter(fmt)
