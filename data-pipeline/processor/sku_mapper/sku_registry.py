"""
sku_mapper/sku_registry.py — In-memory SKU registry with merge and persistence.

Multiple DeduplicatedRecords can map to the same SKU (e.g. same phone listed
on Amazon and Flipkart). This registry merges them and persists the result.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import orjson
from loguru import logger

from models import DeduplicatedRecord, SKURecord
from sku_mapper.sku_builder import sku_builder


class SKURegistry:
    """
    Maintains a canonical set of SKURecords.
    Call register() for each DeduplicatedRecord; call save() at the end.
    """

    def __init__(self) -> None:
        self._registry: dict[str, SKURecord] = {}

    # ------------------------------------------------------------------ public

    def register(self, record: DeduplicatedRecord) -> SKURecord:
        """
        Build a SKU for the given record and merge into the registry.

        Returns:
            The (possibly merged) SKURecord for this record.
        """
        new_sku = sku_builder.build(record)

        if new_sku.sku_id in self._registry:
            existing = self._registry[new_sku.sku_id]
            merged = self._merge(existing, new_sku)
            self._registry[new_sku.sku_id] = merged
            return merged

        self._registry[new_sku.sku_id] = new_sku
        return new_sku

    def register_many(self, records: list[DeduplicatedRecord]) -> None:
        """Register a batch of records."""
        for rec in records:
            self.register(rec)
        logger.info(f"SKU registry: {len(self._registry)} unique SKUs from {len(records)} records")

    def all_skus(self) -> list[SKURecord]:
        return list(self._registry.values())

    def save(self, output_path: Path) -> None:
        """Persist the registry to a JSON file."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "total_skus": len(self._registry),
            "skus": [sku.model_dump(mode="json") for sku in self._registry.values()],
        }
        output_path.write_bytes(orjson.dumps(data, option=orjson.OPT_INDENT_2))
        logger.info(f"Saved {len(self._registry)} SKUs to {output_path}")

    # ------------------------------------------------------------------ private

    def _merge(self, existing: SKURecord, new: SKURecord) -> SKURecord:
        """Merge a new SKU into an existing one, aggregating prices and ratings."""
        # Merge source URLs and domains
        urls = list({*existing.source_urls, *new.source_urls})
        domains = list({*existing.source_domains, *new.source_domains})
        names = list({*existing.product_names, *new.product_names})

        # Price range
        all_prices = [p for p in [
            existing.price_min, existing.price_max,
            new.price_min, new.price_max,
        ] if p is not None]
        price_min = min(all_prices) if all_prices else None
        price_max = max(all_prices) if all_prices else None

        # Weighted average rating
        total_count = (existing.rating_count_total or 0) + (new.rating_count_total or 0)
        if existing.rating_avg and new.rating_avg and total_count > 0:
            ec = existing.rating_count_total or 1
            nc = new.rating_count_total or 1
            rating_avg = round(
                (existing.rating_avg * ec + new.rating_avg * nc) / (ec + nc), 2
            )
        else:
            rating_avg = existing.rating_avg or new.rating_avg

        return SKURecord(
            sku_id=existing.sku_id,
            brand=existing.brand,
            model=existing.model,
            variant=existing.variant,
            category=existing.category or new.category,
            price_min=price_min,
            price_max=price_max,
            price_currency=existing.price_currency,
            rating_avg=rating_avg,
            rating_count_total=total_count,
            source_urls=urls,
            source_domains=domains,
            product_names=names,
            extra=existing.extra,
        )
