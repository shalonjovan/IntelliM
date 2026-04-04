"""
sku_mapper/sku_builder.py — Builds a SKU identifier from a DeduplicatedRecord.

SKU format: {brand_slug}:{model_slug}:{variant_slug}
Example:    samsung:galaxy-s24-ultra:12gb-256gb-titanium-black

Extraction strategy:
  1. Brand — use record.brand (already extracted by scraper), clean to slug
  2. Model — use record.model_number if available, else parse from product_name
  3. Variant — detect storage, RAM, and color from title / bullet points
"""

from __future__ import annotations

import re
from typing import Any

from models import DeduplicatedRecord, SKURecord


# ── Known brand normalizations ───────────────────────────────────────────────
_BRAND_ALIASES: dict[str, str] = {
    "apple": "apple",
    "iphone": "apple",
    "samsung": "samsung",
    "galaxy": "samsung",
    "oneplus": "oneplus",
    "one plus": "oneplus",
    "google": "google",
    "pixel": "google",
    "sony": "sony",
    "lg": "lg",
    "xiaomi": "xiaomi",
    "redmi": "xiaomi",
    "poco": "xiaomi",
    "realme": "realme",
    "oppo": "oppo",
    "vivo": "vivo",
    "nokia": "nokia",
    "motorola": "motorola",
    "moto": "motorola",
    "asus": "asus",
    "lenovo": "lenovo",
    "hp": "hp",
    "dell": "dell",
    "acer": "acer",
    "msi": "msi",
    "bosch": "bosch",
    "bose": "bose",
    "jbl": "jbl",
    "sennheiser": "sennheiser",
    "boat": "boat",
    "nikon": "nikon",
    "canon": "canon",
    "fujifilm": "fujifilm",
    "gopro": "gopro",
}

# ── Variant pattern regexes ──────────────────────────────────────────────────
_STORAGE_RE  = re.compile(r"\b(\d+)\s*(gb|tb)\b", re.I)
_RAM_RE      = re.compile(r"\b(\d+)\s*gb\s*(?:ram|lpddr\d*)\b", re.I)
_COLOR_RE    = re.compile(
    r"\b(black|white|silver|gold|blue|green|red|purple|pink|"
    r"titanium|graphite|starlight|midnight|coral|navy|olive|yellow|orange|gray|grey)\b",
    re.I,
)
_NON_SLUG    = re.compile(r"[^a-z0-9]+")


def _to_slug(text: str) -> str:
    """Convert text to a URL-safe slug."""
    return _NON_SLUG.sub("-", text.lower().strip()).strip("-")


class SKUBuilder:
    """
    Constructs a SKURecord from a DeduplicatedRecord.
    """

    def build(self, record: DeduplicatedRecord) -> SKURecord:
        brand   = self._normalize_brand(record.brand, record.product_name)
        model   = self._extract_model(record)
        variant = self._extract_variant(record)

        sku_id = ":".join(filter(None, [brand, model, variant]))

        return SKURecord(
            sku_id=sku_id,
            brand=brand,
            model=model,
            variant=variant,
            category=record.seed_category,
            price_min=record.price_amount,
            price_max=record.price_amount,
            price_currency=record.price_currency,
            rating_avg=record.rating_value,
            rating_count_total=record.rating_count or 0,
            source_urls=record.source_urls,
            source_domains=[record.source_domain] if record.source_domain else [],
            product_names=[record.product_name] if record.product_name else [],
            extra={
                "sku_hint": record.sku_hint,
                "model_number": record.model_number,
                "merged_count": record.merged_count,
            },
        )

    # ------------------------------------------------------------------ private

    def _normalize_brand(self, brand: str, product_name: str) -> str:
        """Normalize brand to a canonical slug."""
        for alias, canonical in _BRAND_ALIASES.items():
            if alias in brand.lower():
                return canonical
        # Try to extract brand from product name
        for alias, canonical in _BRAND_ALIASES.items():
            if alias in product_name.lower():
                return canonical
        return _to_slug(brand) if brand else "unknown"

    def _extract_model(self, record: DeduplicatedRecord) -> str:
        """
        Extract a clean model slug.
        Uses model_number if available, otherwise strips brand/variant from product_name.
        """
        source = record.model_number or record.product_name or ""
        if not source:
            return "unknown-model"

        # Remove brand prefix if present
        brand_slug = record.brand.lower()
        cleaned = re.sub(r"^\s*" + re.escape(brand_slug) + r"\s*", "", source, flags=re.I).strip()

        # Remove variant-like suffixes (storage/color/ram) to isolate the model
        cleaned = _STORAGE_RE.sub("", cleaned)
        cleaned = _COLOR_RE.sub("", cleaned)
        # Remove bracketed notes
        cleaned = re.sub(r"\([^)]*\)|\[[^\]]*\]", "", cleaned)

        return _to_slug(cleaned) or _to_slug(source)

    def _extract_variant(self, record: DeduplicatedRecord) -> str:
        """
        Extract variant signals (storage, RAM, color) from product name and bullet points.
        """
        text = " ".join([
            record.product_name,
            record.sku_hint,
            *record.bullet_points[:5],
        ])

        parts: list[str] = []

        # RAM + storage
        storage_matches = _STORAGE_RE.findall(text)
        if storage_matches:
            # Take first two (likely RAM + storage)
            for amount, unit in storage_matches[:2]:
                parts.append(f"{amount}{unit.lower()}")

        # Color
        color_match = _COLOR_RE.search(text)
        if color_match:
            parts.append(color_match.group(1).lower())

        return "-".join(parts) if parts else ""


sku_builder = SKUBuilder()
