"""
parser/html_parser.py — Base HTML parser using selectolax.

Extracts common fields from any page:
  - <title>, meta description, og tags
  - JSON-LD structured data blocks
  - Price, rating, brand signals
  - Breadcrumbs, bullet points, main description
"""

from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import urlparse

from loguru import logger
from selectolax.parser import HTMLParser, Node


class HtmlParser:
    """
    Parses raw HTML into a structured dict of extracted fields.
    All extraction is best-effort — missing fields are returned as empty strings / lists.
    """

    # ---------------------------------------------------------------------- public

    def parse(self, html: str, url: str = "") -> dict[str, Any]:
        """
        Parse raw HTML and return a flattened extraction dict.

        Returns:
            dict matching the fields of ParsedContent (excluding url/meta).
        """
        tree = HTMLParser(html)

        result: dict[str, Any] = {}

        result["page_title"]        = self._title(tree)
        result["meta_description"]  = self._meta(tree, "description")
        result["og_title"]          = self._og(tree, "og:title")
        result["og_description"]    = self._og(tree, "og:description")
        result["og_image"]          = self._og(tree, "og:image")
        result["json_ld"]           = self._json_ld(tree)
        result["breadcrumbs"]       = self._breadcrumbs(tree)
        result["bullet_points"]     = self._bullet_points(tree)
        result["description"]       = self._description(tree)

        # Merge JSON-LD derived fields (these override DOM-only extraction)
        ld_fields = self._extract_json_ld_fields(result["json_ld"], url)
        result.update({k: v for k, v in ld_fields.items() if v})

        # DOM-level price/rating/brand (fallback when JSON-LD absent)
        result.setdefault("brand",          self._dom_brand(tree))
        result.setdefault("product_name",   self._dom_product_name(tree))
        result.setdefault("price_raw",      self._dom_price(tree))
        result.setdefault("rating_raw",     self._dom_rating(tree))
        result.setdefault("review_count_raw", self._dom_review_count(tree))
        result.setdefault("availability",   self._dom_availability(tree))
        result.setdefault("sku_raw",        "")
        result.setdefault("model_number",   "")
        result.setdefault("currency_raw",   "")

        # Review-specific
        result.setdefault("review_text",        "")
        result.setdefault("reviewer_name",      "")
        result.setdefault("review_date_raw",    "")
        result.setdefault("verified_purchase",  False)
        result.setdefault("review_rating_raw",  "")

        # Ad-specific
        result.setdefault("advertiser",     "")
        result.setdefault("ad_type",        "")
        result.setdefault("ad_target_url",  "")
        result.setdefault("ad_snippet",     "")

        # Trend-specific
        result.setdefault("trend_rank_raw",  "")
        result.setdefault("trend_score_raw", "")

        # Detected type hint (from JSON-LD @type)
        result["detected_type"] = self._detect_type_from_ld(result["json_ld"])

        return result

    # ------------------------------------------------------------------ private

    def _title(self, tree: HTMLParser) -> str:
        node = tree.css_first("title")
        return node.text(strip=True) if node else ""

    def _meta(self, tree: HTMLParser, name: str) -> str:
        node = tree.css_first(f'meta[name="{name}"]')
        if node:
            return node.attributes.get("content", "")
        return ""

    def _og(self, tree: HTMLParser, prop: str) -> str:
        node = tree.css_first(f'meta[property="{prop}"]')
        if node:
            return node.attributes.get("content", "")
        return ""

    def _json_ld(self, tree: HTMLParser) -> list[dict[str, Any]]:
        """Extract all JSON-LD <script> blocks."""
        results: list[dict[str, Any]] = []
        for node in tree.css('script[type="application/ld+json"]'):
            try:
                data = json.loads(node.text())
                if isinstance(data, list):
                    results.extend(data)
                elif isinstance(data, dict):
                    results.append(data)
            except (json.JSONDecodeError, Exception) as exc:
                logger.debug(f"JSON-LD parse error: {exc}")
        return results

    def _extract_json_ld_fields(
        self, json_ld: list[dict[str, Any]], url: str
    ) -> dict[str, Any]:
        """Pull structured fields from JSON-LD objects."""
        fields: dict[str, Any] = {}
        for obj in json_ld:
            t = obj.get("@type", "")
            types = [t] if isinstance(t, str) else t

            if "Product" in types:
                fields["product_name"]   = obj.get("name", "")
                fields["brand"]          = self._ld_brand(obj)
                fields["description"]    = obj.get("description", "")
                fields["sku_raw"]        = obj.get("sku", "")
                fields["model_number"]   = obj.get("model", "")
                fields["availability"]   = self._ld_availability(obj)
                fields["price_raw"]      = self._ld_price(obj)
                fields["currency_raw"]   = self._ld_currency(obj)
                fields["rating_raw"]     = self._ld_rating(obj)
                fields["review_count_raw"] = self._ld_review_count(obj)

            elif "Review" in types:
                fields["review_text"]       = obj.get("reviewBody", "")
                fields["reviewer_name"]     = self._ld_reviewer(obj)
                fields["review_date_raw"]   = obj.get("datePublished", "")
                fields["review_rating_raw"] = self._ld_review_rating(obj)

            elif "BreadcrumbList" in types:
                items = obj.get("itemListElement", [])
                fields["breadcrumbs"] = [
                    i.get("item", {}).get("name", i.get("name", ""))
                    for i in items
                    if isinstance(i, dict)
                ]

        return fields

    def _ld_brand(self, obj: dict) -> str:
        brand = obj.get("brand", {})
        if isinstance(brand, dict):
            return brand.get("name", "")
        return str(brand)

    def _ld_price(self, obj: dict) -> str:
        offers = obj.get("offers", {})
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        if isinstance(offers, dict):
            return str(offers.get("price", ""))
        return ""

    def _ld_currency(self, obj: dict) -> str:
        offers = obj.get("offers", {})
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        if isinstance(offers, dict):
            return offers.get("priceCurrency", "")
        return ""

    def _ld_availability(self, obj: dict) -> str:
        offers = obj.get("offers", {})
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        if isinstance(offers, dict):
            return offers.get("availability", "")
        return ""

    def _ld_rating(self, obj: dict) -> str:
        agg = obj.get("aggregateRating", {})
        if isinstance(agg, dict):
            return str(agg.get("ratingValue", ""))
        return ""

    def _ld_review_count(self, obj: dict) -> str:
        agg = obj.get("aggregateRating", {})
        if isinstance(agg, dict):
            return str(agg.get("reviewCount", agg.get("ratingCount", "")))
        return ""

    def _ld_reviewer(self, obj: dict) -> str:
        author = obj.get("author", {})
        if isinstance(author, dict):
            return author.get("name", "")
        return str(author)

    def _ld_review_rating(self, obj: dict) -> str:
        rating = obj.get("reviewRating", {})
        if isinstance(rating, dict):
            return str(rating.get("ratingValue", ""))
        return ""

    def _detect_type_from_ld(self, json_ld: list[dict]) -> str:
        for obj in json_ld:
            t = obj.get("@type", "")
            types = ([t] if isinstance(t, str) else t) or []
            if "Product" in types:
                return "Product"
            if "Review" in types:
                return "Review"
            if "SearchResultsPage" in types:
                return "SearchResultsPage"
        return ""

    # ----------------------------------------------------------------- DOM fallbacks

    _PRICE_SELECTORS = [
        ".a-price .a-offscreen",            # Amazon
        "span[data-price]",
        ".price",
        "._30jeq3",                          # Flipkart
        "[class*='price']",
        "[itemprop='price']",
        "meta[itemprop='price']",
    ]

    _RATING_SELECTORS = [
        "span[data-hook='rating-out-of-text']",
        ".a-icon-alt",
        "._3LWZlK",                          # Flipkart
        "[itemprop='ratingValue']",
        "[class*='rating']",
    ]

    _REVIEW_COUNT_SELECTORS = [
        "span[data-hook='total-review-count']",
        "[itemprop='reviewCount']",
        "[class*='review-count']",
        "[class*='reviewCount']",
    ]

    _BRAND_SELECTORS = [
        "a#bylineInfo",                      # Amazon
        "[itemprop='brand']",
        "[class*='brand']",
    ]

    _NAME_SELECTORS = [
        "#productTitle",                     # Amazon
        ".B_NuCI",                           # Flipkart
        "h1[itemprop='name']",
        "h1.product-title",
        "h1",
    ]

    def _dom_price(self, tree: HTMLParser) -> str:
        for sel in self._PRICE_SELECTORS:
            node = tree.css_first(sel)
            if node:
                val = node.attributes.get("content") or node.text(strip=True)
                if val:
                    return val
        return ""

    def _dom_rating(self, tree: HTMLParser) -> str:
        for sel in self._RATING_SELECTORS:
            node = tree.css_first(sel)
            if node:
                return node.text(strip=True)
        return ""

    def _dom_review_count(self, tree: HTMLParser) -> str:
        for sel in self._REVIEW_COUNT_SELECTORS:
            node = tree.css_first(sel)
            if node:
                return node.text(strip=True)
        return ""

    def _dom_brand(self, tree: HTMLParser) -> str:
        for sel in self._BRAND_SELECTORS:
            node = tree.css_first(sel)
            if node:
                return node.text(strip=True)
        return ""

    def _dom_product_name(self, tree: HTMLParser) -> str:
        for sel in self._NAME_SELECTORS:
            node = tree.css_first(sel)
            if node:
                return node.text(strip=True)
        return ""

    def _dom_availability(self, tree: HTMLParser) -> str:
        node = tree.css_first("[itemprop='availability']")
        if node:
            return node.attributes.get("content", node.text(strip=True))
        return ""

    def _breadcrumbs(self, tree: HTMLParser) -> list[str]:
        """Try common breadcrumb selectors."""
        crumbs: list[str] = []
        selectors = [
            "nav[aria-label*='breadcrumb'] a",
            ".a-breadcrumb a",          # Amazon
            "._1MR4o5 a",               # Flipkart
            "[class*='breadcrumb'] a",
            "[class*='Breadcrumb'] a",
            "ol.breadcrumb li",
        ]
        for sel in selectors:
            nodes = tree.css(sel)
            if nodes:
                crumbs = [n.text(strip=True) for n in nodes if n.text(strip=True)]
                if crumbs:
                    break
        return crumbs

    def _bullet_points(self, tree: HTMLParser) -> list[str]:
        """Extract feature bullet points."""
        selectors = [
            "#feature-bullets li",      # Amazon
            "._2418kt li",              # Flipkart spec highlights
            "[class*='feature'] li",
            "[class*='highlight'] li",
        ]
        for sel in selectors:
            nodes = tree.css(sel)
            if nodes:
                bullets = [n.text(strip=True) for n in nodes if n.text(strip=True)]
                if bullets:
                    return bullets[:20]  # cap at 20
        return []

    def _description(self, tree: HTMLParser) -> str:
        """Extract main product description."""
        selectors = [
            "#productDescription",      # Amazon
            "[itemprop='description']",
            ".product-description",
            ".product-overview",
            "[class*='description']",
        ]
        for sel in selectors:
            node = tree.css_first(sel)
            if node:
                text = node.text(strip=True)
                if len(text) > 50:
                    return text[:2000]
        return ""
