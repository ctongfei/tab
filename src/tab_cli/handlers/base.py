"""Base classes for table reading and writing."""

from concurrent.futures import Future, ThreadPoolExecutor
import os
from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass

from loguru import logger
import polars as pl
from rich import box
from rich.progress import Progress, track
from rich.table import Table

from tab_cli import config as config_module
from tab_cli.formats.base import FormatHandler
from tab_cli.storage.base import FileInfo, StorageBackend, has_glob_pattern
from tab_cli.storage.local import LocalBackend
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
            row_styles=[_ALT_ROW_STYLE_0, _ALT_ROW_STYLE_1],
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

    def read(self, url: str) -> pl.LazyFrame:
        return self._scan_files(self._resolve_sources(url))

    def read_preview(self, url: str, limit: int, offset: int = 0) -> pl.LazyFrame:
        files = self._resolve_sources(url)
        if len(files) == 1:
            return self._scan_file(files[0].url).slice(offset, length=limit)
        return self._read_preview_from_files(files, limit=limit, offset=offset)

    def _read_preview_from_files(
        self,
        files: list[FileInfo],
        limit: int,
        offset: int = 0,
    ) -> pl.LazyFrame:
        """Read only as many files as needed to satisfy a preview window."""
        remaining_skip = offset
        remaining_take = limit
        preview_frames: list[pl.DataFrame] = []
        empty_frame: pl.DataFrame | None = None

        for file_info in files:
            if remaining_take <= 0:
                break

            polars_uri = self.backend.normalize_for_polars(file_info.url)
            storage_options = self.backend.storage_options(file_info.url)
            window_size = remaining_skip + remaining_take
            batch = (
                self.format.scan(polars_uri, storage_options=storage_options)
                .slice(0, window_size)
                .collect()
            )

            if empty_frame is None:
                empty_frame = batch.clear()
            if batch.is_empty():
                continue

            if remaining_skip >= len(batch):
                remaining_skip -= len(batch)
                continue

            if remaining_skip > 0:
                batch = batch.slice(remaining_skip, remaining_take)
                remaining_skip = 0
            else:
                batch = batch.head(remaining_take)

            if batch.is_empty():
                continue

            preview_frames.append(batch)
            remaining_take -= len(batch)

        if preview_frames:
            return pl.concat(preview_frames, how="vertical").lazy()
        if empty_frame is not None:
            return empty_frame.lazy()
        return pl.DataFrame().lazy()

    def _resolve_sources(self, url: str) -> list[FileInfo]:
        extension = self.format.extension()
        if has_glob_pattern(url):
            logger.debug(f"Resolving glob input for .{extension} files: {url}")
            files = list(self.backend.list_files(url, extension))
        elif self.backend.is_directory(url):
            logger.debug(f"Resolving directory input for .{extension} files: {url}")
            files = list(self.backend.list_files(url, extension))
        else:
            logger.debug(f"Resolving single-file input: {url}")
            return [FileInfo(url=url, size=self.backend.size(url))]

        if not files:
            raise ValueError(f"No {extension} files found in {url}")
        logger.debug(f"Resolved {len(files)} file(s)")
        return files

    def _scan_file(self, url: str) -> pl.LazyFrame:
        polars_uri = self.backend.normalize_for_polars(url)
        storage_options = self.backend.storage_options(url)
        return self.format.scan(polars_uri, storage_options=storage_options)

    def _scan_files(self, files: list[FileInfo]) -> pl.LazyFrame:
        if len(files) == 1:
            return self._scan_file(files[0].url)

        logger.debug(f"Scanning {len(files)} resolved files")
        frames = [self._scan_file(file_info.url) for file_info in files]
        return pl.concat(frames, how="vertical")

    def schema(self, url: str) -> TableSchema:
        url = self._resolve_sources(url)[0].url
        polars_uri = self.backend.normalize_for_polars(url)
        storage_options = self.backend.storage_options(url)
        columns = self.format.collect_schema(polars_uri, storage_options=storage_options)
        return TableSchema(columns=columns)

    def summary(self, url: str) -> TableSummary:
        files = self._resolve_sources(url)
        if len(files) == 1:
            return self._summary_single(files[0])
        return self._summary_multi_source(url, files)

    def _summary_single(self, file_info: FileInfo) -> TableSummary:
        polars_uri = self.backend.normalize_for_polars(file_info.url)
        storage_options = self.backend.storage_options(file_info.url)
        num_rows = self.format.count_rows(
            file_info.url,
            storage_options=storage_options,
            opener=self.backend.open,
        )
        schema = self.format.collect_schema(polars_uri, storage_options=storage_options)
        num_columns = len(schema)
        extra = self.format.extra_summary(file_info.url)
        return TableSummary(
            file_size=file_info.size,
            num_rows=num_rows,
            num_columns=num_columns,
            extra=extra,
        )

    def _summary_multi_source(self, url: str, files: list[FileInfo]) -> TableSummary:
        """Aggregate summary from all files in a directory or glob input."""
        file_size = 0
        num_rows = 0
        schema_signature: tuple[tuple[str, str], ...] | None = None

        extra_numeric: dict[str, float] = {}
        extra_strings: dict[str, set[str]] = {}
        row_count_futures: dict[str, Future[int]] | None = None

        worker_count = self._summary_worker_count(len(files))
        if worker_count > 1:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                row_count_futures = {
                    file_info.url: executor.submit(self._count_rows_for_summary, file_info.url)
                    for file_info in files
                }
                for file_info in track(files):
                    assert row_count_futures is not None
                    row_count = row_count_futures[file_info.url].result()
                    file_size, num_rows, schema_signature = self._accumulate_summary_file(
                        parent_url=url,
                        file_info=file_info,
                        file_size=file_size,
                        num_rows=num_rows,
                        row_count=row_count,
                        schema_signature=schema_signature,
                        extra_numeric=extra_numeric,
                        extra_strings=extra_strings,
                    )
        else:
            for file_info in track(files):
                row_count = self._count_rows_for_summary(file_info.url)
                file_size, num_rows, schema_signature = self._accumulate_summary_file(
                    parent_url=url,
                    file_info=file_info,
                    file_size=file_size,
                    num_rows=num_rows,
                    row_count=row_count,
                    schema_signature=schema_signature,
                    extra_numeric=extra_numeric,
                    extra_strings=extra_strings,
                )

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
            num_columns=len(schema_signature) if schema_signature is not None else 0,
            extra=result_extra,
        )

    def _accumulate_summary_file(
        self,
        parent_url: str,
        file_info: FileInfo,
        file_size: int,
        num_rows: int,
        row_count: int,
        schema_signature: tuple[tuple[str, str], ...] | None,
        extra_numeric: dict[str, float],
        extra_strings: dict[str, set[str]],
    ) -> tuple[int, int, tuple[tuple[str, str], ...] | None]:
        file_size += file_info.size
        num_rows += row_count

        polars_uri = self.backend.normalize_for_polars(file_info.url)
        storage_options = self.backend.storage_options(file_info.url)
        schema = self.format.collect_schema(polars_uri, storage_options=storage_options)
        current_signature = tuple((name, str(dtype)) for name, dtype in schema)
        if schema_signature is None:
            schema_signature = current_signature
        elif current_signature != schema_signature:
            raise ValueError(f"Inconsistent schema across files in {parent_url}")

        extra = self.format.extra_summary(file_info.url)
        if extra is not None:
            for key, value in extra.items():
                if isinstance(value, (int, float)):
                    extra_numeric[key] = extra_numeric.get(key, 0) + value
                else:
                    extra_strings.setdefault(key, set()).add(str(value))

        return file_size, num_rows, schema_signature

    def _count_rows_for_summary(self, url: str) -> int:
        storage_options = self.backend.storage_options(url)
        return self.format.count_rows(
            url,
            storage_options=storage_options,
            opener=self.backend.open,
        )

    def _summary_worker_count(self, num_files: int) -> int:
        if isinstance(self.backend, LocalBackend):
            return 1
        if config_module.config.num_remote_workers <= 1:
            return 1
        return min(config_module.config.num_remote_workers, num_files)


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
