import logging
import sys

from rich.logging import RichHandler


def setup_logging(level: str = "INFO", log_file: str | None = None) -> None:
    handlers: list[logging.Handler] = [
        RichHandler(rich_tracebacks=True, show_path=False, show_time=False)
    ]
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s %(levelname)s [%(name)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            handlers.append(file_handler)
        except OSError as exc:
            print(f"Warning: could not open log file {log_file}: {exc}", file=sys.stderr)
    logging.basicConfig(
        level=level.upper(),
        format="%(message)s",
        handlers=handlers,
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
