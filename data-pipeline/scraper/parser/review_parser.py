"""
parser/review_parser.py — Extracts customer reviews from review pages.

Handles:
  - Amazon product-reviews pages
  - Flipkart review sections
  - Generic review sections
"""

from __future__ import annotations

import re
from typing import Any

from selectolax.parser import HTMLParser

from parser.html_parser import HtmlParser


class ReviewParser(HtmlParser):
    """
    Review-specific parser. Extracts individual review cards.
    Returns a list of review dicts in result["reviews"].
    """

    def parse(self, html: str, url: str = "") -> dict[str, Any]:
        result = super().parse(html, url)
        tree = HTMLParser(html)

        reviews = self._amazon_reviews(tree)
        if not reviews:
            reviews = self._flipkart_reviews(tree)
        if not reviews:
            reviews = self._generic_reviews(tree)

        result["reviews"] = reviews

        # For single-review pages, populate top-level fields too
        if reviews:
            first = reviews[0]
            result.setdefault("review_text",       first.get("text", ""))
            result.setdefault("reviewer_name",     first.get("reviewer", ""))
            result.setdefault("review_date_raw",   first.get("date", ""))
            result.setdefault("verified_purchase",  first.get("verified", False))
            result.setdefault("review_rating_raw", first.get("rating", ""))

        return result

    # ------------------------------------------------------------------ private

    def _amazon_reviews(self, tree: HTMLParser) -> list[dict[str, Any]]:
        reviews: list[dict[str, Any]] = []
        cards = tree.css("[data-hook='review']")
        for card in cards:
            reviews.append({
                "reviewer":  self._t(card, "[data-hook='review-author']"),
                "rating":    self._t(card, "[data-hook='review-star-rating'] .a-icon-alt"),
                "title":     self._t(card, "[data-hook='review-title']"),
                "date":      self._t(card, "[data-hook='review-date']"),
                "text":      self._t(card, "[data-hook='review-body']"),
                "verified":  bool(card.css_first("[data-hook='avp-badge']")),
                "helpful":   self._t(card, "[data-hook='helpful-vote-statement']"),
            })
        return reviews

    def _flipkart_reviews(self, tree: HTMLParser) -> list[dict[str, Any]]:
        reviews: list[dict[str, Any]] = []
        cards = tree.css("._27M-vq, [class*='review-card']")
        for card in cards:
            reviews.append({
                "reviewer":  self._t(card, "._2sc7ZR"),
                "rating":    self._t(card, "._3LWZlK"),
                "title":     self._t(card, "._2-N8zT"),
                "date":      self._t(card, "._2sc7ZR:last-child"),
                "text":      self._t(card, ".t-ZTKy"),
                "verified":  "Certified Buyer" in card.text(),
                "helpful":   "",
            })
        return reviews

    def _generic_reviews(self, tree: HTMLParser) -> list[dict[str, Any]]:
        reviews: list[dict[str, Any]] = []
        for card in tree.css("[class*='review-item'], [class*='ReviewItem'], [itemprop='review']"):
            reviews.append({
                "reviewer":  self._t(card, "[itemprop='author'], [class*='reviewer']"),
                "rating":    self._t(card, "[itemprop='ratingValue'], [class*='rating']"),
                "title":     self._t(card, "[class*='review-title'], [class*='ReviewTitle']"),
                "date":      self._t(card, "[itemprop='datePublished'], [class*='date']"),
                "text":      self._t(card, "[itemprop='reviewBody'], [class*='review-text'], [class*='ReviewText']"),
                "verified":  False,
                "helpful":   "",
            })
        return reviews

    def _t(self, node, selector: str) -> str:
        """Safe text extraction from first matching child."""
        child = node.css_first(selector)
        return child.text(strip=True) if child else ""
