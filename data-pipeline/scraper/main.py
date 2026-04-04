"""
main.py — CLI entry point for the scraper pipeline.

Usage:
    python main.py                                     # use default storage path
    python main.py --storage-dir ../crawler/storage    # specify crawler storage
    python main.py --output-dir ./output               # where to write JSONL
    python main.py --categories smartphones cameras    # filter by category
    python main.py --dry-run                           # discover pages only
    python main.py --workers 8                         # parallel workers
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from loguru import logger
from rich.console import Console
from rich.table import Table

console = Console()

_DEFAULT_STORAGE = Path(__file__).parent.parent / "crawler" / "storage"
_DEFAULT_OUTPUT  = Path(__file__).parent / "output"


def _configure_logging(level: str) -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        level=level.upper(),
        format=(
            "<green>{time:HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{line}</cyan> — {message}"
        ),
        colorize=True,
    )
    logger.add(
        "scraper.log",
        level="DEBUG",
        rotation="20 MB",
        retention="7 days",
        encoding="utf-8",
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scraper pipeline — parse, clean, classify, normalize crawled pages"
    )
    parser.add_argument(
        "--storage-dir",
        type=Path,
        default=_DEFAULT_STORAGE,
        help=f"Path to crawler storage directory (default: {_DEFAULT_STORAGE})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=_DEFAULT_OUTPUT,
        help=f"Directory to write JSONL output (default: {_DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--categories",
        nargs="*",
        default=[],
        help="Limit scraping to specific categories (e.g. smartphones cameras)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel parsing workers (default: 4)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Discover and log pages without writing output files",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def _print_summary(summary: dict[str, int]) -> None:
    table = Table(title="Scraper Output Summary", show_lines=True)
    table.add_column("File",    style="cyan")
    table.add_column("Records", justify="right", style="green")

    total = 0
    for fname, count in sorted(summary.items()):
        table.add_row(fname, str(count))
        total += count

    table.add_row("[bold]TOTAL[/bold]", f"[bold]{total}[/bold]")
    console.print(table)


async def _main() -> None:
    args = _parse_args()
    _configure_logging(args.log_level)

    from pipeline import ScraperConfig, ScraperPipeline

    if not args.storage_dir.exists():
        console.print(
            f"[red]Storage directory not found: {args.storage_dir}[/red]\n"
            "Run the crawler first: cd ../crawler && python main.py"
        )
        sys.exit(1)

    console.print(f"[bold cyan]Scraper Pipeline[/bold cyan]")
    console.print(f"  Storage : {args.storage_dir}")
    console.print(f"  Output  : {args.output_dir}")
    console.print(f"  Workers : {args.workers}")
    if args.categories:
        console.print(f"  Categories: {', '.join(args.categories)}")
    if args.dry_run:
        console.print("[yellow]  Mode    : DRY RUN[/yellow]")

    cfg = ScraperConfig(
        storage_dir=args.storage_dir,
        output_dir=args.output_dir,
        categories=args.categories or [],
        max_workers=args.workers,
        dry_run=args.dry_run,
    )

    pipeline = ScraperPipeline(cfg)
    summary = await pipeline.run()

    if not args.dry_run and summary:
        _print_summary(summary)
        console.print(
            f"\n[bold green]✓ Output written to {args.output_dir}[/bold green]"
        )


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
