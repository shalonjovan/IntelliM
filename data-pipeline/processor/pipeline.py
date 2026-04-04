"""
pipeline.py — Processor pipeline orchestrator.

Reads scraper JSONL output, then:
  1. Deduplicates product records (field-hash fingerprinting)
  2. Builds SKU identifiers (brand:model:variant slugs)
  3. Builds Category → Brand → Model → Variant hierarchy tree
  4. Exports sku_registry.json + hierarchy.json + hierarchy_flat.jsonl

Usage:
    from processor.pipeline import ProcessorConfig, ProcessorPipeline
    cfg = ProcessorConfig(input_dir=Path("../scraper/output"), output_dir=Path("./output"))
    stats = ProcessorPipeline(cfg).run()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from loguru import logger

from deduplicator.dedup_engine import DedupEngine
from hierarchy.hierarchy_builder import hierarchy_builder
from hierarchy.hierarchy_exporter import hierarchy_exporter
from sku_mapper.sku_registry import SKURegistry


@dataclass
class ProcessorConfig:
    """Configuration for a single processor pipeline run."""
    input_dir: Path       # scraper output dir (contains products.jsonl etc.)
    output_dir: Path      # where to write processor output files
    dry_run: bool = False


class ProcessorPipeline:
    """
    Processor pipeline: dedup → SKU mapping → taxonomy hierarchy → export.
    """

    def __init__(self, config: ProcessorConfig) -> None:
        self.cfg = config
        self._dedup = DedupEngine()

    # ------------------------------------------------------------------ public

    def run(self) -> dict[str, int]:
        """
        Run the full processor pipeline.

        Returns:
            Summary statistics dict.
        """
        stats: dict[str, int] = {}

        # ── Step 1: Deduplicate ──────────────────────────────────────────────
        products_path = self.cfg.input_dir / "products.jsonl"
        deduped_path  = self.cfg.output_dir / "deduped_products.jsonl"

        dedup_stats = self._dedup.run(products_path, deduped_path)
        stats.update(dedup_stats)
        logger.info(
            f"Dedup: {dedup_stats['input']} → {dedup_stats['output']} records "
            f"({dedup_stats['duplicates_removed']} duplicates removed)"
        )

        if self.cfg.dry_run:
            logger.info("Dry run — skipping SKU mapping and hierarchy.")
            return stats

        # ── Step 2: Load deduped records ─────────────────────────────────────
        from models import DeduplicatedRecord
        import orjson

        deduped_records: list[DeduplicatedRecord] = []
        if deduped_path.exists():
            with open(deduped_path, "rb") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        try:
                            deduped_records.append(
                                DeduplicatedRecord.model_validate(orjson.loads(line))
                            )
                        except Exception as exc:
                            logger.debug(f"Skip bad deduped record: {exc}")

        logger.info(f"Loaded {len(deduped_records)} deduped records")
        stats["deduped_loaded"] = len(deduped_records)

        # ── Step 3: Build SKU registry ────────────────────────────────────────
        registry = SKURegistry()
        registry.register_many(deduped_records)

        sku_path = self.cfg.output_dir / "sku_registry.json"
        registry.save(sku_path)
        stats["unique_skus"] = len(registry.all_skus())

        # ── Step 4: Build hierarchy ───────────────────────────────────────────
        all_skus = registry.all_skus()
        roots = hierarchy_builder.build(all_skus)
        h_stats = hierarchy_exporter.export(roots, self.cfg.output_dir)
        stats.update(h_stats)

        logger.info("Processor pipeline complete.")
        return stats
