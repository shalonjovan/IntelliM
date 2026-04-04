"""
parser/product_parser.py — Extends base HTML parser with product-specific extraction.

Handles Amazon, Flipkart, Croma, and generic product pages.
"""

from __future__ import annotations

from typing import Any

from selectolax.parser import HTMLParser

from parser.html_parser import HtmlParser


class ProductParser(HtmlParser):
    """
    Product-specific HTML parser. Extends base extraction with:
    - Variant/color/storage detection
    - Specification table parsing
    - Image URL extraction
    """

    def parse(self, html: str, url: str = "") -> dict[str, Any]:
        result = super().parse(html, url)

        tree = HTMLParser(html)
        result["images"]    = self._images(tree)
        result["variants"]  = self._variants(tree)
        result["specs"]     = self._specs_table(tree)

        # Amazon-specific: seller name
        result["seller"]    = self._amazon_seller(tree)

        # Discount / MRP
        result["mrp_raw"]   = self._mrp(tree)

        return result

    # ------------------------------------------------------------------ private

    def _images(self, tree: HTMLParser) -> list[str]:
        """Extract main product image URLs."""
        urls: list[str] = []
        selectors = [
            "#imgTagWrapperId img",
            "[data-a-dynamic-image]",
            "img[itemprop='image']",
            "img.q-img",
        ]
        for sel in selectors:
            nodes = tree.css(sel)
            for n in nodes:
                src = n.attributes.get("src") or n.attributes.get("data-src", "")
                if src and src.startswith("http") and src not in urls:
                    urls.append(src)
        return urls[:5]

    def _variants(self, tree: HTMLParser) -> list[str]:
        """Detect storage/color/RAM variant options."""
        variants: list[str] = []
        selectors = [
            "#variation_size_name .a-size-base",
            "#variation_color_name .a-size-base",
            "._2H7gBj span",           # Flipkart variants
            "[class*='variant'] span",
            "[class*='Variant'] span",
        ]
        for sel in selectors:
            nodes = tree.css(sel)
            for n in nodes:
                txt = n.text(strip=True)
                if txt and txt not in variants:
                    variants.append(txt)
        return variants[:10]

    def _specs_table(self, tree: HTMLParser) -> dict[str, str]:
        """Extract specification table (key → value)."""
        specs: dict[str, str] = {}
        for table in tree.css(
            "#productDetails_techSpec_section_1, "
            ".a-keyvalue, ._14cfVK, "
            "[class*='spec-table'], [class*='specs']"
        ):
            rows = table.css("tr")
            for row in rows:
                cells = row.css("th, td")
                if len(cells) >= 2:
                    key = cells[0].text(strip=True)
                    val = cells[1].text(strip=True)
                    if key and val:
                        specs[key] = val
        return specs

    def _amazon_seller(self, tree: HTMLParser) -> str:
        node = tree.css_first("#sellerProfileTriggerId, #merchant-info a")
        return node.text(strip=True) if node else ""

    def _mrp(self, tree: HTMLParser) -> str:
        selectors = [
            ".a-text-price .a-offscreen",
            "._3I9_wc",                # Flipkart MRP
            "[class*='mrp']",
            "[class*='original-price']",
        ]
        for sel in selectors:
            node = tree.css_first(sel)
            if node:
                return node.text(strip=True)
        return ""
