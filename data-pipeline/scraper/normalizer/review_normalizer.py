"""
normalizer/review_normalizer.py — Produces NormalizedReview records from ParsedContent.
Handles both single-review pages and pages with multiple review cards.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from cleaner.rating_cleaner import rating_cleaner
from cleaner.text_cleaner import text_cleaner
from models import NormalizedReview, RatingInfo


class ReviewNormalizer:
    """Converts parsed review fields into one or more NormalizedReview records."""

    def normalize_many(
        self,
        fields: dict[str, Any],
        url: str,
        seed_category: str = "",
        crawled_at: datetime | None = None,
    ) -> list[NormalizedReview]:
        """
        Returns a list of NormalizedReview items.
        If the page has multiple review cards (fields["reviews"]), each becomes a record.
        Otherwise, produces one record from the top-level fields.
        """
        domain = urlparse(url).netloc
        at = crawled_at or datetime.utcnow()

        reviews_raw: list[dict] = fields.get("reviews", [])

        if reviews_raw:
            return [
                self._from_card(card, url, domain, seed_category, at)
                for card in reviews_raw
            ]

        # Single-review page
        return [self._from_top_level(fields, url, domain, seed_category, at)]

    # ------------------------------------------------------------------ private

    def _from_card(self, card: dict, url: str, domain: str, category: str, at: datetime) -> NormalizedReview:
        rating_data = rating_cleaner.clean(card.get("rating", ""))
        return NormalizedReview(
            url=url,
            source_domain=domain,
            seed_category=category,
            reviewer_name=text_cleaner.clean_short(card.get("reviewer", "")),
            review_text=text_cleaner.clean_long(card.get("text", "")),
            review_date=text_cleaner.clean_short(card.get("date", "")),
            verified_purchase=bool(card.get("verified", False)),
            rating=RatingInfo(
                value=rating_data.get("value"),
                raw=card.get("rating", ""),
            ),
            crawled_at=at,
            extra={"title": text_cleaner.clean_short(card.get("title", ""))},
        )

    def _from_top_level(self, fields: dict, url: str, domain: str, category: str, at: datetime) -> NormalizedReview:
        rating_data = rating_cleaner.clean(fields.get("review_rating_raw", ""))
        return NormalizedReview(
            url=url,
            source_domain=domain,
            seed_category=category,
            reviewer_name=text_cleaner.clean_short(fields.get("reviewer_name", "")),
            review_text=text_cleaner.clean_long(fields.get("review_text", "")),
            review_date=text_cleaner.clean_short(fields.get("review_date_raw", "")),
            verified_purchase=fields.get("verified_purchase", False),
            rating=RatingInfo(
                value=rating_data.get("value"),
                raw=fields.get("review_rating_raw", ""),
            ),
            crawled_at=at,
        )


review_normalizer = ReviewNormalizer()
