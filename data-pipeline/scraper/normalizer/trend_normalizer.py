"""
normalizer/trend_normalizer.py — Produces NormalizedTrend records from ParsedContent.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from cleaner.text_cleaner import text_cleaner
from models import NormalizedTrend


class TrendNormalizer:
    """Converts parsed trend fields into NormalizedTrend records (one per ranked item)."""

    def normalize_many(
        self,
        fields: dict[str, Any],
        url: str,
        seed_category: str = "",
        crawled_at: datetime | None = None,
    ) -> list[NormalizedTrend]:
        domain = urlparse(url).netloc
        at = crawled_at or datetime.utcnow()

        trend_items: list[dict] = fields.get("trend_items", [])

        if trend_items:
            return [
                NormalizedTrend(
                    url=url,
                    source_domain=domain,
                    seed_category=seed_category,
                    rank=item.get("rank"),
                    product_name=text_cleaner.clean_short(item.get("name", "")),
                    trend_score=item.get("score"),
                    category=seed_category,
                    crawled_at=at,
                    extra={"price": item.get("price", ""), "item_url": item.get("url", "")},
                )
                for item in trend_items
                if item.get("name")
            ]

        # Single summary record from top-level fields
        rank_str = fields.get("trend_rank_raw", "")
        rank = None
        if rank_str:
            m = re.search(r"\d+", rank_str)
            rank = int(m.group()) if m else None

        return [
            NormalizedTrend(
                url=url,
                source_domain=domain,
                seed_category=seed_category,
                rank=rank,
                product_name=text_cleaner.clean_short(
                    fields.get("product_name", "") or fields.get("page_title", "")
                ),
                trend_score=None,
                category=seed_category,
                crawled_at=at,
            )
        ]


trend_normalizer = TrendNormalizer()
