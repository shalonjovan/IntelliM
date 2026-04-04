"""
classifier/content_classifier.py — Re-classifies parsed content into ContentType.

Priority order:
  1. JSON-LD @type hint (most reliable — schema.org structured data)
  2. Crawler page_type_hint (from URL/DOM rules in the crawler pipeline)
  3. DOM structural signals (price block, review block, ranking list)
  4. Falls back to OTHER
"""

from __future__ import annotations

import re

from loguru import logger

from models import ContentType


_PAGE_TYPE_MAP: dict[str, ContentType] = {
    "product_page":   ContentType.PRODUCT,
    "review_page":    ContentType.REVIEW,
    "ad_creative":    ContentType.AD,
    "trend_data":     ContentType.TREND,
    "category_page":  ContentType.CATEGORY,
    "unknown":        ContentType.OTHER,
}

_LD_TYPE_MAP: dict[str, ContentType] = {
    "product":              ContentType.PRODUCT,
    "review":               ContentType.REVIEW,
    "itempage":             ContentType.PRODUCT,
    "searchresultspage":    ContentType.CATEGORY,
    "webpage":              ContentType.OTHER,
}


class ContentClassifier:
    """
    Classifies parsed page content into a ContentType.
    Stateless — one instance shared across the whole scraper run.
    """

    def classify(
        self,
        detected_type: str,        # from JSON-LD parser
        page_type_hint: str,       # from crawler classifier
        parsed_fields: dict,       # full parsed dict for DOM signal fallback
    ) -> ContentType:
        """
        Determine ContentType from available signals.

        Args:
            detected_type: JSON-LD @type string (e.g. "Product", "Review")
            page_type_hint: crawler page_type string (e.g. "product_page")
            parsed_fields: full ParsedContent-like dict for dom signal fallback

        Returns:
            ContentType
        """
        # 1. JSON-LD @type — highest confidence
        if detected_type:
            key = detected_type.lower()
            ct = _LD_TYPE_MAP.get(key)
            if ct:
                logger.debug(f"Classified via JSON-LD @type='{detected_type}' → {ct.value}")
                return ct

        # 2. Crawler page_type hint
        hint_key = (page_type_hint or "").lower()
        if hint_key in _PAGE_TYPE_MAP and _PAGE_TYPE_MAP[hint_key] is not ContentType.OTHER:
            ct = _PAGE_TYPE_MAP[hint_key]
            logger.debug(f"Classified via page_type_hint='{hint_key}' → {ct.value}")
            return ct

        # 3. DOM structural signals
        ct = self._dom_signals(parsed_fields)
        if ct:
            logger.debug(f"Classified via DOM signals → {ct.value}")
            return ct

        logger.debug("Content type undetermined → OTHER")
        return ContentType.OTHER

    # ------------------------------------------------------------------ private

    def _dom_signals(self, fields: dict) -> ContentType | None:
        """Use rich parsed fields as classification signals."""

        # Strong product signals
        if fields.get("price_raw") and (fields.get("product_name") or fields.get("brand")):
            return ContentType.PRODUCT

        # Add-to-cart or buy-now in bullet points / description
        body = " ".join([
            fields.get("description", ""),
            fields.get("meta_description", ""),
            *fields.get("bullet_points", []),
        ])
        if re.search(r"add to (cart|bag)|buy now|check out|checkout", body, re.I):
            return ContentType.PRODUCT

        # Strong review signals
        if fields.get("review_text") and len(fields.get("review_text", "")) > 30:
            return ContentType.REVIEW
        reviews = fields.get("reviews", [])
        if len(reviews) >= 2:
            return ContentType.REVIEW

        # Trend/rank signals
        trend_items = fields.get("trend_items", [])
        if len(trend_items) >= 3:
            return ContentType.TREND
        if re.search(r"bestseller|#\d+\s+in\s+|trending|movers", body, re.I):
            return ContentType.TREND

        # Ad signals
        if fields.get("advertiser") or re.search(r"sponsored|ad transparency", body, re.I):
            return ContentType.AD

        return None
