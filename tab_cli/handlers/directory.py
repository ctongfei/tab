"""Directory handler for partitioned datasets."""

import os
from collections.abc import Iterable
from glob import glob

import polars as pl

from tab_cli.handlers.base import TableReader, TableSchema, TableSummary


class DirectoryReader(TableReader):
    """Handler wrapper for partitioned datasets (directories of files)."""

    def __init__(self, extension: str, file_handler: TableReader | type[TableReader]) -> None:
        self.extension = extension
        self.file_handler = file_handler

    def _get_files(self, path: str) -> list[str]:
        """Get all files with matching extension in the directory."""
        pattern = os.path.join(path, "**", f"*{self.extension}")
        return sorted(glob(pattern, recursive=True))

    def read(self, path: str, limit: int | None = None, offset: int = 0) -> pl.LazyFrame:
        """Read data from a partitioned dataset."""
        files = self._get_files(path)
        if not files:
            raise ValueError(f"No {self.extension} files found in {path}")

        frames = [self.file_handler.read(file) for file in files]
        lf = pl.concat(frames, how="vertical")

        if offset > 0:
            lf = lf.slice(offset, length=limit)
        elif limit is not None:
            lf = lf.head(limit)

        return lf

    def schema(self, path: str) -> TableSchema:
        """Get the schema from the partitioned dataset."""
        files = self._get_files(path)
        if not files:
            raise ValueError(f"No {self.extension} files found in {path}")
        return self.file_handler.schema(files[0])

    def summary(self, path: str) -> TableSummary:
        """Get aggregated summary from all partition files."""
        files = self._get_files(path)
        if not files:
            raise ValueError(f"No {self.extension} files found in {path}")

        file_size = 0
        num_rows = 0
        num_columns: int | None = None

        extra_numeric: dict[str, float] = {}
        extra_strings: dict[str, set[str]] = {}

        for file in files:
            file_summary = self.file_handler.summary(file)
            file_size += file_summary.file_size
            num_rows += file_summary.num_rows

            if num_columns is None:
                num_columns = file_summary.num_columns
            elif file_summary.num_columns != num_columns:
                raise ValueError(f"Inconsistent column counts in {path}")

            if file_summary.extra:
                for key, value in file_summary.extra.items():
                    if isinstance(value, (int, float)):
                        extra_numeric[key] = extra_numeric.get(key, 0) + value
                    else:
                        extra_strings.setdefault(key, set()).add(str(value))

        extra: dict[str, str | int | float] = {"Partitions": len(files)}
        for key, value in extra_numeric.items():
            if float(value).is_integer():
                extra[key] = int(value)
            else:
                extra[key] = value

        for key, values in extra_strings.items():
            if len(values) == 1:
                extra[key] = next(iter(values))
            else:
                extra[key] = ", ".join(sorted(values))

        return TableSummary(
            file_size=file_size,
            num_rows=num_rows,
            num_columns=num_columns or 0,
            extra=extra,
        )

