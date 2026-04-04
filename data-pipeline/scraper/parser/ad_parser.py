"""
parser/ad_parser.py — Extracts information from ad / sponsored content pages.
"""

from __future__ import annotations

from typing import Any

from selectolax.parser import HTMLParser

from parser.html_parser import HtmlParser


class AdParser(HtmlParser):
    """
    Ad-specific parser. Extracts advertiser, ad type, target URL, and snippet.
    """

    def parse(self, html: str, url: str = "") -> dict[str, Any]:
        result = super().parse(html, url)
        tree = HTMLParser(html)

        result["advertiser"]    = self._advertiser(tree, result)
        result["ad_type"]       = self._ad_type(tree, url)
        result["ad_target_url"] = self._target_url(tree)
        result["ad_snippet"]    = self._snippet(tree, result)

        return result

    # ------------------------------------------------------------------ private

    def _advertiser(self, tree: HTMLParser, result: dict) -> str:
        # Try brand from base parser first
        if result.get("brand"):
            return result["brand"]
        # Sponsored brand header
        node = tree.css_first(
            ".s-sponsored-label-info-icon, "
            "[class*='sponsored-label'], "
            "[class*='advertiser']"
        )
        return node.text(strip=True) if node else ""

    def _ad_type(self, tree: HTMLParser, url: str) -> str:
        if "ad-transparency" in url or "adtransparency" in url:
            return "transparency_page"
        if tree.css_first("[data-component-type='sp-sponsored-result']"):
            return "sponsored_product"
        if tree.css_first(".s-sponsored-label"):
            return "sponsored_listing"
        return "unknown"

    def _target_url(self, tree: HTMLParser) -> str:
        for sel in [
            "[data-click-el='BodyText'] a",
            ".s-product-image-container a",
            "[class*='ad-link']",
        ]:
            node = tree.css_first(sel)
            if node:
                href = node.attributes.get("href", "")
                if href:
                    return href
        return ""

    def _snippet(self, tree: HTMLParser, result: dict) -> str:
        # Use page description or OG description as snippet
        return (
            result.get("og_description")
            or result.get("meta_description")
            or result.get("description", "")
        )[:500]
