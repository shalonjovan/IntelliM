"""
main.py — CLI entry point for the processor pipeline.

Usage:
    python main.py                                      # use default input/output paths
    python main.py --input-dir ../scraper/output        # scraper JSONL directory
    python main.py --output-dir ./output                # processor output directory
    python main.py --dry-run                            # dedup only, skip SKU+hierarchy
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger
from rich.console import Console
from rich.table import Table

console = Console()

_DEFAULT_INPUT  = Path(__file__).parent.parent / "scraper" / "output"
_DEFAULT_OUTPUT = Path(__file__).parent / "output"


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
        "processor.log",
        level="DEBUG",
        rotation="20 MB",
        retention="7 days",
        encoding="utf-8",
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Processor pipeline — deduplicate, SKU map, and build hierarchy"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=_DEFAULT_INPUT,
        help=f"Scraper output directory (default: {_DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=_DEFAULT_OUTPUT,
        help=f"Processor output directory (default: {_DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Run deduplication only — skip SKU mapping and hierarchy",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def _print_summary(stats: dict[str, int]) -> None:
    table = Table(title="Processor Output Summary", show_lines=True)
    table.add_column("Metric",  style="cyan")
    table.add_column("Value",   justify="right", style="green")

    labels = {
        "input":              "Product records (input)",
        "output":             "After deduplication",
        "duplicates_removed": "Duplicates removed",
        "deduped_loaded":     "Deduped records loaded",
        "unique_skus":        "Unique SKUs",
        "nodes":              "Hierarchy nodes",
        "leaves":             "Hierarchy leaf nodes",
    }
    for key, label in labels.items():
        if key in stats:
            table.add_row(label, str(stats[key]))

    console.print(table)


def main() -> None:
    args = _parse_args()
    _configure_logging(args.log_level)

    from pipeline import ProcessorConfig, ProcessorPipeline

    if not args.input_dir.exists():
        console.print(
            f"[red]Input directory not found: {args.input_dir}[/red]\n"
            "Run the scraper first: cd ../scraper && python main.py"
        )
        sys.exit(1)

    console.print(f"[bold cyan]Processor Pipeline[/bold cyan]")
    console.print(f"  Input   : {args.input_dir}")
    console.print(f"  Output  : {args.output_dir}")
    if args.dry_run:
        console.print("[yellow]  Mode    : DRY RUN (dedup only)[/yellow]")

    cfg = ProcessorConfig(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        dry_run=args.dry_run,
    )

    pipeline = ProcessorPipeline(cfg)
    stats = pipeline.run()

    _print_summary(stats)

    output_files = [
        args.output_dir / "deduped_products.jsonl",
        args.output_dir / "sku_registry.json",
        args.output_dir / "hierarchy.json",
        args.output_dir / "hierarchy_flat.jsonl",
    ]

    console.print("\n[bold]Output files:[/bold]")
    for f in output_files:
        exists = "✓" if f.exists() else "✗"
        color = "green" if f.exists() else "red"
        console.print(f"  [{color}]{exists}[/{color}] {f}")

    console.print(f"\n[bold green]✓ Processing complete.[/bold green]")


if __name__ == "__main__":
    main()
