"""Base reader interface for tabular data."""

import os
from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from rich.table import Table
from rich.progress import Progress
from rich import box

import polars as pl

from tab_cli.style import _KEY_STYLE, _VAL_STYLE, _ALT_ROW_STYLE


@dataclass
class TableSchema:
    """Schema information for a table."""

    columns: list[tuple[str, pl.DataType]]

    def __rich__(self) -> Table:
        """Rich-formatted output for the schema."""

        table = Table(
            show_header=False,
            box=box.SIMPLE_HEAD,
            row_styles=["", _ALT_ROW_STYLE],
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
        """Rich-formatted output for the summary."""

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


class TableReader(ABC):

    @abstractmethod
    def read(self, path: str, limit: int | None = None, offset: int = 0) -> pl.LazyFrame:
        pass

    @abstractmethod
    def schema(self, path: str) -> TableSchema:
        pass

    @abstractmethod
    def summary(self, path: str) -> TableSummary:
        pass


class TableWriter(ABC):

    @abstractmethod
    def extension(self) -> str:
        """Return the file extension for this format (e.g., '.parquet', '.csv')."""
        pass

    @abstractmethod
    def write(self, lf: pl.LazyFrame) -> Iterable[bytes]:
        pass

    @abstractmethod
    def write_single(self, lf: pl.LazyFrame, path: str) -> None:
        """Write a LazyFrame to a single file."""
        pass

    def write_to_path(self, lf: pl.LazyFrame, path: str, partitions: int | None = None) -> None:
        """Write a LazyFrame to a file or partitioned directory."""
        if partitions is None:
            with Progress() as progress:
                task = progress.add_task("Writing...", total=1)
                self.write_single(lf, path)
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
                        self.write_single(part_lf, part_path)
                    progress.update(task, advance=1)
