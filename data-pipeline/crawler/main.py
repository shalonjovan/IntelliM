"""
main.py — CLI entry point.

Usage:
    python main.py                          # crawl all categories
    python main.py --categories smartphones laptops
    python main.py --use-playwright         # enable JS rendering
    python main.py --concurrency 3          # override concurrency
    python main.py --dry-run                # load seeds only, don't crawl
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from loguru import logger
from rich.console import Console
from rich.table import Table

from config.settings import settings
from models import Category
from seeds.seed_loader import load_all_seeds

console = Console()


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
        "crawl.log",
        level="DEBUG",
        rotation="50 MB",
        retention="7 days",
        encoding="utf-8",
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Portable electronics competitor tracking crawler"
    )
    parser.add_argument(
        "--categories",
        nargs="*",
        choices=[c.value for c in Category],
        default=None,
        help="Limit crawl to specific categories (default: all)",
    )
    parser.add_argument(
        "--use-playwright",
        action="store_true",
        default=False,
        help="Use Playwright browser instead of httpx (slower but handles JS)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help=f"Max parallel requests (default: {settings.max_concurrency})",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=None,
        help=f"Max crawl depth (default: {settings.global_max_depth})",
    )
    parser.add_argument(
        "--seed-dir",
        type=Path,
        default=None,
        help="Path to seed config directory (default: config/seed_configs/)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Load and validate seeds only — do not start the crawler",
    )
    parser.add_argument(
        "--log-level",
        default=settings.log_level,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def _print_seed_summary(seed_configs) -> None:
    table = Table(title="Loaded Seed Configs", show_lines=True)
    table.add_column("Category",      style="cyan")
    table.add_column("Products",      justify="right")
    table.add_column("Domains",       justify="right")
    table.add_column("Start URLs",    justify="right")
    table.add_column("Max Depth",     justify="right")

    for cfg in seed_configs:
        table.add_row(
            cfg.category.value,
            str(len(cfg.products)),
            str(len(cfg.domains)),
            str(len(cfg.start_urls)),
            str(cfg.max_depth),
        )

    console.print(table)
    total_products = sum(len(c.products) for c in seed_configs)
    console.print(f"[bold green]Total tracked products: {total_products}[/bold green]")


async def _main() -> None:
    args = _parse_args()
    _configure_logging(args.log_level)

    # Apply CLI overrides to settings
    if args.use_playwright:
        settings.use_playwright = True
    if args.concurrency is not None:
        settings.max_concurrency = args.concurrency
    if args.max_depth is not None:
        settings.global_max_depth = args.max_depth

    # Load seeds
    seed_dir = args.seed_dir or settings.seed_configs_dir
    all_configs = load_all_seeds(seed_dir)

    # Filter to requested categories
    if args.categories:
        requested = set(args.categories)
        all_configs = [c for c in all_configs if c.category.value in requested]
        if not all_configs:
            logger.error(f"No seed configs found for categories: {requested}")
            sys.exit(1)

    _print_seed_summary(all_configs)

    if args.dry_run:
        console.print("[yellow]Dry run — exiting before crawl starts.[/yellow]")
        return

    # Run the pipeline
    from pipeline import run
    await run(seed_configs=all_configs)


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()