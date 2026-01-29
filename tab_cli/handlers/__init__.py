"""Handler registration and inference."""

import os

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


def _get_extension(path: str) -> str:
    """Extract file extension from a path or URL."""
    # Handle URIs by getting the path component
    if "://" in path:
        path = path.split("://", 1)[1]
    # Get extension from basename
    basename = os.path.basename(path.rstrip("/"))
    return os.path.splitext(basename)[1][1:].lower()


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
        # Get extension from first file in directory
        for file_info in backend.list_files(path, ""):
            extension = _get_extension(file_info.url)
            if extension:
                break
        else:
            raise ValueError(f"No files found in directory: {path}")
    else:
        extension = _get_extension(path)

    fmt = _FORMAT_MAP.get(extension)
    if fmt is None:
        raise ValueError(f"Unknown extension: {extension}. Supported: {', '.join(_FORMAT_MAP)}")

    return TableReader(backend, fmt)


def infer_writer(format: str | None = None, truncated: bool = False) -> TableWriter:
    """Infer the writer for a format.

    Args:
        format: Output format. If None, returns CLI table formatter.
        truncated: Whether the output is truncated (for CLI display).

    Returns:
        TableWriter for the format.
    """
    if format is None:
        return CliTableFormatter(truncated=truncated)
    if format == "table-svg":
        return CliTableFormatter(truncated=truncated, svg_capture=True)

    fmt = _FORMAT_MAP.get(format.lower())
    if fmt is None:
        raise ValueError(f"Unknown format: {format}. Supported: {', '.join(_FORMAT_MAP)}")

    return FormatWriter(fmt)
