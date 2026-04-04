"""
parser/trend_parser.py — Extracts trend/bestseller data from ranking pages.

Handles:
  - Amazon bestsellers / movers & shakers
  - Flipkart trending / most-popular pages
  - Generic ranked product lists
"""

from __future__ import annotations

import re
from typing import Any

from selectolax.parser import HTMLParser

from parser.html_parser import HtmlParser


class TrendParser(HtmlParser):
    """
    Trend-specific parser. Returns result["trend_items"] — a list of ranked items.
    """

    def parse(self, html: str, url: str = "") -> dict[str, Any]:
        result = super().parse(html, url)
        tree = HTMLParser(html)

        trend_items = self._amazon_bestsellers(tree)
        if not trend_items:
            trend_items = self._flipkart_trending(tree)
        if not trend_items:
            trend_items = self._generic_ranked(tree)

        result["trend_items"] = trend_items

        # Populate top-level fields from the first ranked item
        if trend_items:
            first = trend_items[0]
            result.setdefault("trend_rank_raw",  str(first.get("rank", "")))
            result.setdefault("trend_score_raw", str(first.get("score", "")))

        return result

    # ------------------------------------------------------------------ private

    def _amazon_bestsellers(self, tree: HTMLParser) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        cards = tree.css(
            ".zg-grid-general-faceout, "
            ".p13n-grid-content, "
            "[class*='zg_itemWrapper']"
        )
        for i, card in enumerate(cards, start=1):
            name_node = card.css_first("span.p13n-sc-truncated, ._cDEzb_p13n-sc-css-line-clamp-3_g3dy1")
            rank_node = card.css_first(".zg-bdg-text, [class*='rank']")
            price_node = card.css_first("span.p13n-sc-price, .a-price .a-offscreen")
            items.append({
                "rank":  int(re.sub(r"\D", "", rank_node.text()) or i) if rank_node else i,
                "name":  name_node.text(strip=True) if name_node else "",
                "price": price_node.text(strip=True) if price_node else "",
                "score": None,
                "url":   self._card_url(card),
            })
        return items

    def _flipkart_trending(self, tree: HTMLParser) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        cards = tree.css("._1AtVbE, ._2kHMtA")
        for i, card in enumerate(cards, start=1):
            name_node = card.css_first("._4rR01T, .IRpwTa")
            price_node = card.css_first("._30jeq3")
            items.append({
                "rank":  i,
                "name":  name_node.text(strip=True) if name_node else "",
                "price": price_node.text(strip=True) if price_node else "",
                "score": None,
                "url":   self._card_url(card),
            })
        return items

    def _generic_ranked(self, tree: HTMLParser) -> list[dict[str, Any]]:
        """Fallback: find ordered list of products."""
        items: list[dict[str, Any]] = []
        cards = tree.css("[class*='product-card'], [class*='product-item'], li.item")
        for i, card in enumerate(cards, start=1):
            name = card.css_first("h2, h3, .name, .title")
            items.append({
                "rank":  i,
                "name":  name.text(strip=True) if name else "",
                "price": "",
                "score": None,
                "url":   self._card_url(card),
            })
        return items

    def _card_url(self, card) -> str:
        link = card.css_first("a")
        return link.attributes.get("href", "") if link else ""
