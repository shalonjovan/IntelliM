"""
deduplicator/dedup_engine.py — Reads product JSONL records and deduplicates them.

Strategy:
  1. For each record, compute a field fingerprint (brand + model + category)
  2. Group records by fingerprint
  3. For each group, keep the "richest" record (most non-null/non-empty fields)
     and append all source URLs to it

The engine only deduplicates PRODUCT-type records.
Review/ad/trend records are passed through unchanged (they don't need dedup).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import orjson
from loguru import logger

from deduplicator.fingerprinter import field_fingerprint, normalize_url
from models import DeduplicatedRecord


class DedupEngine:
    """
    Reads products.jsonl, deduplicates, and writes deduped_products.jsonl.
    """

    def run(
        self,
        input_path: Path,
        output_path: Path,
    ) -> dict[str, int]:
        """
        Deduplicate products from input_path and write to output_path.

        Returns:
            {"input": N, "output": M, "duplicates_removed": D}
        """
        records = self._load(input_path)
        logger.info(f"Loaded {len(records)} product records from {input_path}")

        deduped = self._deduplicate(records)
        logger.info(
            f"Deduplication: {len(records)} → {len(deduped)} records "
            f"({len(records) - len(deduped)} duplicates removed)"
        )

        self._write(deduped, output_path)
        return {
            "input":              len(records),
            "output":             len(deduped),
            "duplicates_removed": len(records) - len(deduped),
        }

    # ------------------------------------------------------------------ private

    def _load(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            logger.warning(f"Input file not found: {path}")
            return []
        records: list[dict[str, Any]] = []
        with open(path, "rb") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(orjson.loads(line))
                    except Exception as exc:
                        logger.debug(f"Bad JSON line: {exc}")
        return records

    def _deduplicate(
        self, records: list[dict[str, Any]]
    ) -> list[DeduplicatedRecord]:
        """Group by content fingerprint and merge each group into one record."""
        groups: dict[str, list[dict[str, Any]]] = {}

        for rec in records:
            fp = field_fingerprint(
                brand=rec.get("brand", ""),
                product_name=rec.get("product_name", ""),
                model_number=rec.get("model_number", ""),
                price_amount=self._price(rec),
                seed_category=rec.get("seed_category", ""),
            )
            groups.setdefault(fp, []).append(rec)

        result: list[DeduplicatedRecord] = []
        for fp, group in groups.items():
            merged = self._merge_group(fp, group)
            result.append(merged)

        return result

    def _merge_group(
        self, fingerprint: str, group: list[dict[str, Any]]
    ) -> DeduplicatedRecord:
        """
        Merge a group of duplicate records into one canonical record.
        Keeps the record with the most populated fields as the base,
        then fills in any empty fields from the other records.
        """
        # Sort by richness (most non-empty fields first)
        ranked = sorted(group, key=self._richness, reverse=True)
        base = ranked[0]

        # Collect all source URLs (normalized)
        all_urls = list({
            normalize_url(r.get("url", ""))
            for r in group
            if r.get("url")
        })

        # Fill missing fields from secondary records
        for r in ranked[1:]:
            for field in ["product_name", "brand", "model_number", "sku_hint", "description", "availability"]:
                if not base.get(field) and r.get(field):
                    base[field] = r[field]
            if not base.get("bullet_points") and r.get("bullet_points"):
                base["bullet_points"] = r["bullet_points"]

        price = base.get("price", {})
        return DeduplicatedRecord(
            content_hash=fingerprint,
            url_normalized=normalize_url(base.get("url", "")),
            source_urls=all_urls,
            content_type=base.get("content_type", "product"),
            seed_category=base.get("seed_category", ""),
            source_domain=base.get("source_domain", ""),
            product_name=base.get("product_name", ""),
            brand=base.get("brand", ""),
            model_number=base.get("model_number", ""),
            sku_hint=base.get("sku_hint", ""),
            description=base.get("description", ""),
            availability=base.get("availability", ""),
            breadcrumbs=base.get("breadcrumbs", []),
            bullet_points=base.get("bullet_points", []),
            price_amount=price.get("amount") if isinstance(price, dict) else None,
            price_currency=(price.get("currency", "INR") if isinstance(price, dict) else "INR"),
            rating_value=self._rating_val(base),
            rating_count=self._rating_count(base),
            extra=base.get("extra", {}),
            merged_count=len(group),
        )

    def _write(self, records: list[DeduplicatedRecord], path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as fh:
            for rec in records:
                fh.write(orjson.dumps(rec.model_dump(mode="json")) + b"\n")
        logger.info(f"Wrote {len(records)} deduped records to {path}")

    @staticmethod
    def _richness(rec: dict) -> int:
        """Count non-empty field values."""
        return sum(1 for v in rec.values() if v not in (None, "", [], {}))

    @staticmethod
    def _price(rec: dict) -> float | None:
        p = rec.get("price", {})
        if isinstance(p, dict):
            return p.get("amount")
        return None

    @staticmethod
    def _rating_val(rec: dict) -> float | None:
        r = rec.get("rating", {})
        if isinstance(r, dict):
            return r.get("value")
        return None

    @staticmethod
    def _rating_count(rec: dict) -> int | None:
        r = rec.get("rating", {})
        if isinstance(r, dict):
            count = r.get("count")
            return int(count) if count is not None else None
        return None


dedup_engine = DedupEngine()
