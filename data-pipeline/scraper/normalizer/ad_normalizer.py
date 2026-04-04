"""
normalizer/ad_normalizer.py — Produces NormalizedAd from ParsedContent.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from cleaner.text_cleaner import text_cleaner
from models import NormalizedAd


class AdNormalizer:
    def normalize(
        self,
        fields: dict[str, Any],
        url: str,
        seed_category: str = "",
        crawled_at: datetime | None = None,
    ) -> NormalizedAd:
        domain = urlparse(url).netloc
        return NormalizedAd(
            url=url,
            source_domain=domain,
            seed_category=seed_category,
            advertiser=text_cleaner.clean_short(fields.get("advertiser", "")),
            ad_type=text_cleaner.clean_short(fields.get("ad_type", "")),
            target_url=fields.get("ad_target_url", ""),
            content_snippet=text_cleaner.clean(fields.get("ad_snippet", ""), max_len=500),
            crawled_at=crawled_at or datetime.utcnow(),
        )


ad_normalizer = AdNormalizer()
