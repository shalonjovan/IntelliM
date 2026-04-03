"""
classifier/classifier_rules.py — URL-pattern and DOM-signal rule sets
used by the page classifier to assign a PageType to any given page.

Rules are evaluated in order; the first match wins.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable

from classifier.page_types import PageType


# ---------------------------------------------------------------------------
# Rule primitives
# ---------------------------------------------------------------------------

@dataclass
class UrlRule:
    """A regex pattern applied to the full URL."""
    pattern: re.Pattern[str]
    page_type: PageType
    description: str = ""

    def matches(self, url: str) -> bool:
        return bool(self.pattern.search(url))


@dataclass
class DomRule:
    """
    A heuristic applied to the page's HTML text / DOM signals.
    The check callable receives the raw HTML string and returns True on match.
    """
    check: Callable[[str], bool]
    page_type: PageType
    description: str = ""

    def matches(self, html: str) -> bool:
        return self.check(html)


# ---------------------------------------------------------------------------
# URL rules (ordered — first match wins)
# ---------------------------------------------------------------------------

_URL_RULES: list[UrlRule] = [

    # ── Review pages ────────────────────────────────────────────────────────
    UrlRule(
        re.compile(
            r"/(?:reviews?|ratings?|user-review|customer-review|product-review"
            r"|write-a-review|all-reviews)[/?#]",
            re.I,
        ),
        PageType.REVIEW_PAGE,
        "URL contains review-related segment",
    ),
    # Amazon reviews
    UrlRule(
        re.compile(r"amazon\.[a-z.]+/.*#customerReviews", re.I),
        PageType.REVIEW_PAGE,
        "Amazon customer reviews anchor",
    ),
    UrlRule(
        re.compile(r"amazon\.[a-z.]+/product-reviews/", re.I),
        PageType.REVIEW_PAGE,
        "Amazon product-reviews path",
    ),
    # Flipkart reviews
    UrlRule(
        re.compile(r"flipkart\.com/.*[?&]reviewPage=", re.I),
        PageType.REVIEW_PAGE,
        "Flipkart reviewPage query param",
    ),

    # ── Ad / sponsored pages ────────────────────────────────────────────────
    UrlRule(
        re.compile(
            r"/(?:ads?|sponsored|ad-transparency|ad-disclosure|advertising)[/?#]",
            re.I,
        ),
        PageType.AD_CREATIVE,
        "URL contains ad/sponsored segment",
    ),

    # ── Trend / search-trend pages ───────────────────────────────────────────
    UrlRule(
        re.compile(
            r"/(?:trends?|search-trends?|popular|bestseller|best-seller"
            r"|movers-shakers|new-releases?)[/?#]",
            re.I,
        ),
        PageType.TREND_DATA,
        "URL contains trend/popularity segment",
    ),
    # Amazon movers & shakers / best sellers
    UrlRule(
        re.compile(r"amazon\.[a-z.]+/(?:gp/)?(?:movers-and-shakers|bestsellers?)", re.I),
        PageType.TREND_DATA,
        "Amazon bestsellers/movers-shakers",
    ),

    # ── Product pages ────────────────────────────────────────────────────────
    # Amazon /dp/ or /gp/product/
    UrlRule(
        re.compile(r"amazon\.[a-z.]+/(?:[^/]+/)?(?:dp|gp/product)/[A-Z0-9]{10}", re.I),
        PageType.PRODUCT_PAGE,
        "Amazon product ASIN pattern",
    ),
    # Flipkart product URLs contain /p/
    UrlRule(
        re.compile(r"flipkart\.com/[^/]+/p/itm[a-z0-9]+", re.I),
        PageType.PRODUCT_PAGE,
        "Flipkart product itm-id",
    ),
    # Croma product URLs
    UrlRule(
        re.compile(r"croma\.com/[^/]+-p\d+", re.I),
        PageType.PRODUCT_PAGE,
        "Croma -p<id> product pattern",
    ),
    # Generic product path signals
    UrlRule(
        re.compile(
            r"/(?:product|item|sku|pdp|buy|detail)[s]?/[^/?#]+",
            re.I,
        ),
        PageType.PRODUCT_PAGE,
        "Generic product URL segment",
    ),

    # ── Category / listing pages ─────────────────────────────────────────────
    # Amazon search results
    UrlRule(
        re.compile(r"amazon\.[a-z.]+/s[/?]", re.I),
        PageType.CATEGORY_PAGE,
        "Amazon search/listing page",
    ),
    # Flipkart category SID
    UrlRule(
        re.compile(r"flipkart\.com/[^/]+/pr\?sid=", re.I),
        PageType.CATEGORY_PAGE,
        "Flipkart category sid",
    ),
    # Generic listing/category signals
    UrlRule(
        re.compile(
            r"/(?:category|categories|collection|listing|search|browse|c/\d+|l/)[/?#]?",
            re.I,
        ),
        PageType.CATEGORY_PAGE,
        "Generic category/listing URL segment",
    ),
]


# ---------------------------------------------------------------------------
# DOM rules (applied when URL rules produce UNKNOWN)
# ---------------------------------------------------------------------------

def _has_add_to_cart(html: str) -> bool:
    return bool(re.search(r"add.to.cart|buy.now|add-to-bag", html, re.I))

def _has_review_section(html: str) -> bool:
    return bool(re.search(
        r"customer.reviews?|write a review|star rating|verified purchase",
        html, re.I,
    ))

def _has_product_grid(html: str) -> bool:
    # Multiple product cards suggests a listing page
    count = len(re.findall(r"product.?card|item.?tile|s-result-item", html, re.I))
    return count >= 3

def _has_trend_signals(html: str) -> bool:
    return bool(re.search(
        r"trending|most popular|bestseller|#\d+ in |movers and shakers",
        html, re.I,
    ))

def _has_ad_signals(html: str) -> bool:
    return bool(re.search(
        r"sponsored content|ad transparency|this ad|why this ad|about this ad",
        html, re.I,
    ))


_DOM_RULES: list[DomRule] = [
    DomRule(_has_ad_signals,      PageType.AD_CREATIVE,   "DOM: ad transparency signals"),
    DomRule(_has_trend_signals,   PageType.TREND_DATA,    "DOM: trending/bestseller signals"),
    DomRule(_has_review_section,  PageType.REVIEW_PAGE,   "DOM: review section present"),
    DomRule(_has_add_to_cart,     PageType.PRODUCT_PAGE,  "DOM: add-to-cart button"),
    DomRule(_has_product_grid,    PageType.CATEGORY_PAGE, "DOM: product grid (3+ cards)"),
]


# ---------------------------------------------------------------------------
# Public accessors
# ---------------------------------------------------------------------------

def get_url_rules() -> list[UrlRule]:
    return _URL_RULES


def get_dom_rules() -> list[DomRule]:
    return _DOM_RULES