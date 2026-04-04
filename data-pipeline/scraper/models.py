"""
models.py — shared Pydantic models for the scraper pipeline.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Content type enum (scraper-level, richer than the crawler's PageType)
# ---------------------------------------------------------------------------

class ContentType(str, Enum):
    PRODUCT     = "product"      # product detail page
    REVIEW      = "review"       # customer review page / review section
    AD          = "ad"           # sponsored / ad-transparency page
    TREND       = "trend"        # bestseller / trend / popularity page
    CATEGORY    = "category"     # category / listing page (not scraped further)
    OTHER       = "other"        # could not classify


# ---------------------------------------------------------------------------
# Input model — wraps a single crawled page
# ---------------------------------------------------------------------------

class ScrapedPage(BaseModel):
    """One crawled page fed into the scraper pipeline."""
    url: str
    html: str
    page_type_hint: str = "unknown"          # from crawler classifier
    seed_category: str = "unknown"           # e.g. smartphones, cameras
    seed_domain: str = ""
    depth: int = 0
    crawled_at: datetime = Field(default_factory=datetime.utcnow)
    extra: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Intermediate model — raw fields extracted from HTML
# ---------------------------------------------------------------------------

class ParsedContent(BaseModel):
    """Structured fields extracted from raw HTML — not yet normalized."""
    url: str
    page_title: str = ""
    meta_description: str = ""
    og_title: str = ""
    og_description: str = ""
    og_image: str = ""

    # Product signals
    product_name: str = ""
    brand: str = ""
    price_raw: str = ""               # e.g. "₹1,49,999"
    currency_raw: str = ""
    rating_raw: str = ""              # e.g. "4.2 out of 5"
    review_count_raw: str = ""
    availability: str = ""
    sku_raw: str = ""
    model_number: str = ""
    breadcrumbs: list[str] = Field(default_factory=list)
    bullet_points: list[str] = Field(default_factory=list)
    description: str = ""

    # Review signals
    review_text: str = ""
    reviewer_name: str = ""
    review_date_raw: str = ""
    verified_purchase: bool = False
    review_rating_raw: str = ""

    # Ad signals
    advertiser: str = ""
    ad_type: str = ""
    ad_target_url: str = ""
    ad_snippet: str = ""

    # Trend signals
    trend_rank_raw: str = ""
    trend_score_raw: str = ""

    # JSON-LD structured data (raw dict)
    json_ld: list[dict[str, Any]] = Field(default_factory=list)

    # Detected page type from JSON-LD / DOM signals
    detected_type: str = ""

    # Metadata
    seed_category: str = ""
    seed_domain: str = ""
    crawled_at: datetime = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Normalized records — one per content type
# ---------------------------------------------------------------------------

class PriceInfo(BaseModel):
    amount: float | None = None
    currency: str = "INR"
    raw: str = ""


class RatingInfo(BaseModel):
    value: float | None = None       # 0–5 scale
    count: int | None = None
    raw: str = ""


class NormalizedProduct(BaseModel):
    content_type: ContentType = ContentType.PRODUCT
    url: str
    source_domain: str = ""
    seed_category: str = ""

    product_name: str = ""
    brand: str = ""
    model_number: str = ""
    sku_hint: str = ""
    breadcrumbs: list[str] = Field(default_factory=list)
    bullet_points: list[str] = Field(default_factory=list)
    description: str = ""
    availability: str = ""

    price: PriceInfo = Field(default_factory=PriceInfo)
    rating: RatingInfo = Field(default_factory=RatingInfo)

    crawled_at: datetime = Field(default_factory=datetime.utcnow)
    extra: dict[str, Any] = Field(default_factory=dict)


class NormalizedReview(BaseModel):
    content_type: ContentType = ContentType.REVIEW
    url: str
    source_domain: str = ""
    seed_category: str = ""

    parent_product_url: str = ""
    reviewer_name: str = ""
    review_text: str = ""
    review_date: str = ""
    verified_purchase: bool = False
    rating: RatingInfo = Field(default_factory=RatingInfo)

    crawled_at: datetime = Field(default_factory=datetime.utcnow)
    extra: dict[str, Any] = Field(default_factory=dict)


class NormalizedAd(BaseModel):
    content_type: ContentType = ContentType.AD
    url: str
    source_domain: str = ""
    seed_category: str = ""

    advertiser: str = ""
    ad_type: str = ""
    target_url: str = ""
    content_snippet: str = ""

    crawled_at: datetime = Field(default_factory=datetime.utcnow)
    extra: dict[str, Any] = Field(default_factory=dict)


class NormalizedTrend(BaseModel):
    content_type: ContentType = ContentType.TREND
    url: str
    source_domain: str = ""
    seed_category: str = ""

    rank: int | None = None
    product_name: str = ""
    trend_score: float | None = None
    category: str = ""

    crawled_at: datetime = Field(default_factory=datetime.utcnow)
    extra: dict[str, Any] = Field(default_factory=dict)


class NormalizedOther(BaseModel):
    content_type: ContentType = ContentType.OTHER
    url: str
    source_domain: str = ""
    seed_category: str = ""
    page_title: str = ""
    description: str = ""
    crawled_at: datetime = Field(default_factory=datetime.utcnow)


# Union for type hints
NormalizedRecord = (
    NormalizedProduct
    | NormalizedReview
    | NormalizedAd
    | NormalizedTrend
    | NormalizedOther
)
