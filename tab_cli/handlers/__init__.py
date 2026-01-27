import os

from tab_cli.handlers.base import TableReader, TableWriter
from tab_cli.handlers.cli_table import CliTableFormatter
from tab_cli.handlers.csv import CsvHandler
from tab_cli.handlers.directory import DirectoryReader
from tab_cli.handlers.jsonl import JsonlWriter
from tab_cli.handlers.parquet import ParquetHandler

_READER_MAP = {
    "csv": CsvHandler(","),
    "tsv": CsvHandler("\t"),
    "parquet": ParquetHandler(),
}

_WRITER_MAP = {
    "csv": CsvHandler(","),
    "tsv": CsvHandler("\t"),
    "parquet": ParquetHandler(),
    "jsonl": JsonlWriter(),
}

def infer_reader(path: str, format: str | None = None) -> TableReader:
    """Infer the handler for a file. If format is given, use that instead of extension."""
    if format is not None:
        handler = _READER_MAP.get(format.lower())
        if handler is None:
            raise ValueError(f"Unknown format: {format}. Supported: {', '.join(_READER_MAP)}")
        return handler

    if os.path.isdir(path):
        extension = os.path.splitext(os.listdir(path)[0])[1][1:].lower()
        return DirectoryReader(extension, infer_reader_from_extension(extension))

    extension = os.path.splitext(path)[1][1:].lower()
    return infer_reader_from_extension(extension)


def infer_reader_from_extension(extension: str) -> TableReader:
    """Infer the handler for a file based on its extension."""
    handler = _READER_MAP.get(extension)
    if handler is None:
        raise ValueError(f"Unknown extension: {extension}. Supported: {', '.join(_READER_MAP)}")
    return handler


def infer_writer(format: str | None = None, truncated: bool = False) -> TableWriter:
    """Infer the writer for a format."""
    if format is None:
        return CliTableFormatter(truncated=truncated)
    handler = _WRITER_MAP.get(format.lower())
    if handler is None:
        raise ValueError(f"Unknown format: {format}. Supported: {', '.join(_WRITER_MAP)}")
    return handler
