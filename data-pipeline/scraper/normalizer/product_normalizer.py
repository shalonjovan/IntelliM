"""
normalizer/product_normalizer.py — Produces a NormalizedProduct from ParsedContent.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from cleaner.price_cleaner import price_cleaner
from cleaner.rating_cleaner import rating_cleaner
from cleaner.text_cleaner import text_cleaner
from models import NormalizedProduct, PriceInfo, RatingInfo


class ProductNormalizer:
    """Converts a parsed field dict into a canonical NormalizedProduct."""

    def normalize(
        self,
        fields: dict[str, Any],
        url: str,
        seed_category: str = "",
        crawled_at: datetime | None = None,
    ) -> NormalizedProduct:
        domain = urlparse(url).netloc

        # Price
        price_data = price_cleaner.clean(fields.get("price_raw", ""))
        price = PriceInfo(
            amount=price_data["amount"],
            currency=price_data.get("currency", "INR"),
            raw=price_data.get("raw", ""),
        )

        # Rating
        rating_data = rating_cleaner.clean(fields.get("rating_raw", ""))
        count_raw = fields.get("review_count_raw", "")
        count = rating_cleaner.clean_count(count_raw)
        rating = RatingInfo(
            value=rating_data.get("value"),
            count=count,
            raw=fields.get("rating_raw", ""),
        )

        # Extra enrichment fields from product parser
        extra: dict[str, Any] = {}
        if fields.get("specs"):
            extra["specs"] = fields["specs"]
        if fields.get("variants"):
            extra["variants"] = fields["variants"]
        if fields.get("images"):
            extra["images"] = fields["images"]
        if fields.get("seller"):
            extra["seller"] = text_cleaner.clean_short(fields["seller"])
        if fields.get("mrp_raw"):
            mrp = price_cleaner.clean(fields["mrp_raw"])
            extra["mrp"] = mrp

        return NormalizedProduct(
            url=url,
            source_domain=domain,
            seed_category=seed_category,
            product_name=text_cleaner.clean_short(fields.get("product_name", "") or fields.get("og_title", "") or fields.get("page_title", "")),
            brand=text_cleaner.clean_short(fields.get("brand", "")),
            model_number=text_cleaner.clean_short(fields.get("model_number", "")),
            sku_hint=text_cleaner.clean_short(fields.get("sku_raw", "")),
            breadcrumbs=text_cleaner.clean_list(fields.get("breadcrumbs", [])),
            bullet_points=text_cleaner.clean_list(fields.get("bullet_points", [])),
            description=text_cleaner.clean_long(fields.get("description", "") or fields.get("meta_description", "")),
            availability=text_cleaner.clean_short(fields.get("availability", "")),
            price=price,
            rating=rating,
            crawled_at=crawled_at or datetime.utcnow(),
            extra=extra,
        )


product_normalizer = ProductNormalizer()
