"""
models.py — shared Pydantic models used across the entire pipeline.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
from urllib.parse import urlparse

from pydantic import BaseModel, Field, HttpUrl, field_validator


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Category(str, Enum):
    SMARTPHONES       = "smartphones"
    LAPTOPS           = "laptops"
    EARPHONES         = "earphones"
    SMARTWATCHES      = "smartwatches"
    TABLETS           = "tablets"
    CAMERAS           = "cameras"
    PORTABLE_SPEAKERS = "portable_speakers"


# ---------------------------------------------------------------------------
# Seed models
# ---------------------------------------------------------------------------

class SeedProduct(BaseModel):
    """One tracked product inside a seed JSON file."""
    name: str
    brand: str
    model_id: str                        # internal identifier, e.g. "samsung-s24-ultra"
    search_keywords: list[str] = Field(default_factory=list)
    known_urls: list[str] = Field(default_factory=list)  # optional pre-known product URLs


class SeedConfig(BaseModel):
    """Top-level structure of every seed JSON file."""
    category: Category
    version: str = "1.0"
    domains: list[str]                   # e.g. ["amazon.in", "flipkart.com"]
    start_urls: list[str]                # category-level entry URLs
    products: list[SeedProduct]
    max_depth: int = Field(default=4, ge=1, le=10)
    respect_robots: bool = True

    @field_validator("domains")
    @classmethod
    def strip_scheme(cls, v: list[str]) -> list[str]:
        cleaned = []
        for d in v:
            parsed = urlparse(d if "://" in d else f"https://{d}")
            cleaned.append(parsed.netloc or parsed.path)
        return cleaned

    @field_validator("start_urls")
    @classmethod
    def validate_urls(cls, v: list[str]) -> list[str]:
        for url in v:
            if not url.startswith(("http://", "https://")):
                raise ValueError(f"start_url must be absolute: {url}")
        return v


# ---------------------------------------------------------------------------
# Crawl context — attached to every request as user_data
# ---------------------------------------------------------------------------

class CrawlMeta(BaseModel):
    """
    Context object propagated with every request through the queue.
    This is what prevents the crawler from straying off-seed.
    """
    seed_category: Category
    seed_domain: str                     # root domain this request belongs to
    origin_url: str                      # the start_url that spawned this chain
    depth: int = 0
    max_depth: int = 4
    parent_url: str | None = None
    product_hint: str | None = None      # model_id if we know which product this is for
    crawled_at: datetime = Field(default_factory=datetime.utcnow)
    extra: dict[str, Any] = Field(default_factory=dict)

    @property
    def is_at_max_depth(self) -> bool:
        return self.depth >= self.max_depth

    def child(self, url: str) -> "CrawlMeta":
        """Return a new CrawlMeta for a discovered child URL."""
        return self.model_copy(
            update={
                "depth": self.depth + 1,
                "parent_url": self.origin_url if self.depth == 0 else self.parent_url,
                "crawled_at": datetime.utcnow(),
                "extra": {},
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CrawlMeta":
        return cls.model_validate(data)