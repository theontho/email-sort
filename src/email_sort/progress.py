from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.text import Text


class RateColumn(ProgressColumn):
    """Render task throughput as iterations per second."""

    def render(self, task):
        speed = task.speed or 0.0
        return Text(f"{speed:.2f} it/s", style="bold blue")


def progress_columns(*, spinner: bool = False, bar_width: int | None = 40):
    columns = []
    if spinner:
        columns.append(SpinnerColumn())
    columns.extend(
        [
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=bar_width),
            MofNCompleteColumn(),
            RateColumn(),
            TextColumn("elapsed:"),
            TimeElapsedColumn(),
            TextColumn("eta:"),
            TimeRemainingColumn(),
        ]
    )
    return columns


def make_progress(
    *,
    spinner: bool = False,
    bar_width: int | None = 40,
    console: Console | None = None,
    expand: bool = False,
    transient: bool = False,
) -> Progress:
    return Progress(
        *progress_columns(spinner=spinner, bar_width=bar_width),
        console=console,
        expand=expand,
        transient=transient,
    )
