"""
models.py — Pydantic models for the processor pipeline.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

class DeduplicatedRecord(BaseModel):
    """A product record with a deduplication fingerprint attached."""
    content_hash: str                # sha256 fingerprint for dedup
    url_normalized: str              # URL after stripping tracking params
    source_urls: list[str] = Field(default_factory=list)   # all source URLs for this record
    content_type: str = "product"
    seed_category: str = ""
    source_domain: str = ""

    # Product-level fields (populated for PRODUCT records)
    product_name: str = ""
    brand: str = ""
    model_number: str = ""
    sku_hint: str = ""
    description: str = ""
    availability: str = ""
    breadcrumbs: list[str] = Field(default_factory=list)
    bullet_points: list[str] = Field(default_factory=list)

    price_amount: float | None = None
    price_currency: str = "INR"
    rating_value: float | None = None
    rating_count: int | None = None

    extra: dict[str, Any] = Field(default_factory=dict)
    crawled_at: datetime = Field(default_factory=datetime.utcnow)
    merged_count: int = 1           # how many duplicates were merged into this record


# ---------------------------------------------------------------------------
# SKU
# ---------------------------------------------------------------------------

class SKURecord(BaseModel):
    """Canonical product identifier with aggregated details."""
    sku_id: str                      # e.g. "samsung:galaxy-s24-ultra:12gb-256gb"
    brand: str
    model: str                       # clean model name
    variant: str = ""                # storage/color/ram variant string
    category: str = ""

    # Aggregated pricing across sources
    price_min: float | None = None
    price_max: float | None = None
    price_currency: str = "INR"

    # Aggregated ratings
    rating_avg: float | None = None
    rating_count_total: int = 0

    # All source URLs that resolved to this SKU
    source_urls: list[str] = Field(default_factory=list)
    source_domains: list[str] = Field(default_factory=list)

    # Raw signals used to build this SKU
    product_names: list[str] = Field(default_factory=list)
    crawled_at: datetime = Field(default_factory=datetime.utcnow)
    extra: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Hierarchy
# ---------------------------------------------------------------------------

class HierarchyNode(BaseModel):
    """A node in the product taxonomy tree."""
    name: str
    node_type: str                   # "category" | "brand" | "model" | "variant"
    slug: str = ""                   # url-safe identifier
    children: list["HierarchyNode"] = Field(default_factory=list)
    sku_id: str | None = None        # only on leaf (variant) nodes
    data: dict[str, Any] = Field(default_factory=dict)   # price, rating, urls etc.


HierarchyNode.model_rebuild()  # resolve forward ref
