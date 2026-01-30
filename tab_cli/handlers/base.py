"""Base classes for table reading and writing."""

import os
from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass

import polars as pl
from rich import box
from rich.progress import Progress
from rich.table import Table

from tab_cli.formats.base import FormatHandler
from tab_cli.storage.base import StorageBackend
from tab_cli.style import _ALT_ROW_STYLE_0, _ALT_ROW_STYLE_1, _KEY_STYLE, _VAL_STYLE


@dataclass
class TableSchema:
    """Schema information for a table."""

    columns: list[tuple[str, pl.DataType]]

    def __rich__(self) -> Table:
        table = Table(
            show_header=False,
            box=box.SIMPLE_HEAD,
            row_styles=[_ALT_ROW_STYLE_0, _ALT_ROW_STYLE_1],
        )
        table.add_column(style=_KEY_STYLE)
        table.add_column(style=_VAL_STYLE)
        for name, dtype in self.columns:
            table.add_row(name, str(dtype))
        return table


@dataclass
class TableSummary:
    """Summary information for a table."""

    file_size: int
    num_rows: int
    num_columns: int
    extra: dict[str, str | int | float] | None = None

    def __rich__(self) -> Table:
        def format_size(size: int) -> str:
            s: float = size
            for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
                if s < 1024:
                    return f"{s:.1f} {unit}" if unit != "B" else f"{int(s)} {unit}"
                s /= 1024
            return f"{s:.1f} PiB"

        table = Table(
            show_header=False,
            box=box.SIMPLE_HEAD,
            row_styles=["", _ALT_ROW_STYLE],
        )
        table.add_column(style=_KEY_STYLE)
        table.add_column(style=_VAL_STYLE)

        table.add_row("File size", format_size(self.file_size))
        table.add_row("Rows", f"{self.num_rows:,}")
        table.add_row("Columns", str(self.num_columns))

        if self.extra:
            for key, value in self.extra.items():
                table.add_row(key, str(value))

        return table


class TableReader:
    """Reads tabular data by composing a StorageBackend and FormatHandler."""

    def __init__(self, backend: StorageBackend, format: FormatHandler):
        self.backend = backend
        self.format = format

    def read(self, url: str, limit: int | None = None, offset: int = 0) -> pl.LazyFrame:
        if self.backend.is_directory(url):
            lf = self._read_directory(url)
        else:
            polars_uri = self.backend.normalize_for_polars(url)
            storage_options = self.backend.storage_options(url)
            lf = self.format.scan(polars_uri, storage_options=storage_options)

        if offset > 0:
            lf = lf.slice(offset, length=limit)
        elif limit is not None:
            lf = lf.head(limit)
        return lf

    def _read_directory(self, url: str) -> pl.LazyFrame:
        """Read all files in a directory."""
        extension = self.format.extension()
        storage_options = self.backend.storage_options(url)

        if self.format.supports_glob():
            # Use native glob support
            polars_uri = self.backend.normalize_for_polars(url)
            glob_pattern = os.path.join(polars_uri, "**", f"*{extension}")
            return self.format.scan(glob_pattern, storage_options=storage_options)
        else:
            # Manual concatenation for formats without glob support
            files = list(self.backend.list_files(url, extension))
            if not files:
                raise ValueError(f"No {extension} files found in {url}")
            frames = [
                self.format.scan(
                    self.backend.normalize_for_polars(f.url),
                    storage_options=self.backend.storage_options(f.url),
                )
                for f in files
            ]
            return pl.concat(frames, how="vertical")

    def schema(self, url: str) -> TableSchema:
        if self.backend.is_directory(url):
            # Get schema from first file
            files = list(self.backend.list_files(url, self.format.extension()))
            if not files:
                raise ValueError(f"No {self.format.extension()} files found in {url}")
            url = files[0].url
        polars_uri = self.backend.normalize_for_polars(url)
        storage_options = self.backend.storage_options(url)
        columns = self.format.collect_schema(polars_uri, storage_options=storage_options)
        return TableSchema(columns=columns)

    def summary(self, url: str) -> TableSummary:
        if self.backend.is_directory(url):
            return self._summary_directory(url)
        else:
            return self._summary_single(url)

    def _summary_single(self, url: str) -> TableSummary:
        file_size = self.backend.size(url)
        polars_uri = self.backend.normalize_for_polars(url)
        storage_options = self.backend.storage_options(url)
        num_rows = self.format.count_rows(polars_uri, storage_options=storage_options)
        schema = self.format.collect_schema(polars_uri, storage_options=storage_options)
        num_columns = len(schema)
        extra = self.format.extra_summary(url)
        return TableSummary(
            file_size=file_size,
            num_rows=num_rows,
            num_columns=num_columns,
            extra=extra,
        )

    def _summary_directory(self, url: str) -> TableSummary:
        """Aggregate summary from all files in directory."""
        files = list(self.backend.list_files(url, self.format.extension()))
        if not files:
            raise ValueError(f"No {self.format.extension()} files found in {url}")

        file_size = 0
        num_rows = 0
        num_columns: int | None = None

        extra_numeric: dict[str, float] = {}
        extra_strings: dict[str, set[str]] = {}

        for file_info in files:
            file_size += file_info.size
            polars_uri = self.backend.normalize_for_polars(file_info.url)
            storage_options = self.backend.storage_options(file_info.url)
            num_rows += self.format.count_rows(polars_uri, storage_options=storage_options)

            schema = self.format.collect_schema(polars_uri, storage_options=storage_options)
            if num_columns is None:
                num_columns = len(schema)
            elif len(schema) != num_columns:
                raise ValueError(f"Inconsistent column counts in {url}")

            extra = self.format.extra_summary(file_info.url)
            if extra:
                for key, value in extra.items():
                    if isinstance(value, (int, float)):
                        extra_numeric[key] = extra_numeric.get(key, 0) + value
                    else:
                        extra_strings.setdefault(key, set()).add(str(value))

        result_extra: dict[str, str | int | float] = {"Partitions": len(files)}
        for key, value in extra_numeric.items():
            if float(value).is_integer():
                result_extra[key] = int(value)
            else:
                result_extra[key] = value

        for key, values in extra_strings.items():
            if len(values) == 1:
                result_extra[key] = next(iter(values))
            else:
                result_extra[key] = ", ".join(sorted(values))

        return TableSummary(
            file_size=file_size,
            num_rows=num_rows,
            num_columns=num_columns or 0,
            extra=result_extra,
        )


class TableWriter(ABC):
    """Base class for writing tabular data."""

    @abstractmethod
    def extension(self) -> str:
        """Return the file extension for this format."""
        pass

    @abstractmethod
    def write(self, lf: pl.LazyFrame) -> Iterable[bytes]:
        """Write LazyFrame to bytes (for streaming output)."""
        pass

    @abstractmethod
    def write_to_single_file(self, lf: pl.LazyFrame, path: str) -> None:
        """Write LazyFrame to a single file."""
        pass

    def write_to_path(self, lf: pl.LazyFrame, path: str, partitions: int | None = None) -> None:
        """Write LazyFrame to a file or partitioned directory."""
        if partitions is None:
            with Progress() as progress:
                task = progress.add_task("Writing...", total=1)
                self.write_to_single_file(lf, path)
                progress.update(task, completed=1)
        else:
            os.makedirs(path, exist_ok=True)
            row_count = lf.select(pl.len()).collect().item()
            rows_per_part = (row_count + partitions - 1) // partitions
            with Progress() as progress:
                task = progress.add_task("Writing partitions...", total=partitions)
                for i in range(partitions):
                    offset = i * rows_per_part
                    if offset < row_count:
                        part_lf = lf.slice(offset, rows_per_part)
                        part_path = os.path.join(path, f"part-{i:05d}{self.extension()}")
                        self.write_to_single_file(part_lf, part_path)
                    progress.update(task, advance=1)


class FormatWriter(TableWriter):
    """TableWriter adapter for FormatHandler."""

    def __init__(self, format: FormatHandler):
        self._format = format

    def extension(self) -> str:
        return self._format.extension()

    def write(self, lf: pl.LazyFrame) -> Iterable[bytes]:
        return self._format.write(lf)

    def write_to_single_file(self, lf: pl.LazyFrame, path: str) -> None:
        self._format.write_to_single_file(lf, path)
