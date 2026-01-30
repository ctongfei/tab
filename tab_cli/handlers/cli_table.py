import sys
from collections.abc import Iterable

from rich.table import Table
from rich import box
from rich.console import Console
import polars as pl

from tab_cli.handlers.base import TableWriter
from tab_cli.style import _ALT_ROW_STYLE_0, _ALT_ROW_STYLE_1, _KEY_STYLE


class CliTableFormatter(TableWriter):
    def __init__(self, truncated: bool = False, svg_capture: bool = False):
        self.truncated = truncated
        self.svg_capture = svg_capture

    def extension(self) -> str:
        return ".txt"

    def write(self, lf: pl.LazyFrame) -> Iterable[bytes]:

        table = Table(
            show_header=True,
            header_style=_KEY_STYLE,
            box=box.SIMPLE_HEAD,
            row_styles=[_ALT_ROW_STYLE_0, _ALT_ROW_STYLE_1],
        )

        for col in lf.collect_schema().names():
            table.add_column(col)

        for batch in lf.collect_batches():
            for row in batch.iter_rows():
                table.add_row(*[str(v) if v is not None else "" for v in row])

        if self.truncated:
            table.add_row(*["..." for _ in lf.collect_schema().names()])

        if self.svg_capture:
            console = Console(record=True, width=80)
            console.print(table)
            svg = console.export_svg()
            print(svg, file=sys.stderr)
        else:
            console = Console()
            with console.capture() as capture:
                console.print(table)
            yield capture.get().encode("utf-8")

    def write_to_single_file(self, lf: pl.LazyFrame, path: str) -> None:
        """Write a LazyFrame to a single text file."""
        with open(path, "wb") as f:
            for chunk in self.write(lf):
                f.write(chunk)
