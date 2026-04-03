"""
crawler/link_extractor.py — extracts and normalises all anchor hrefs
from a fetched HTML page.

Uses selectolax (fast C-based parser) to pull <a href> values, then
normalises them to absolute URLs and strips noise.
"""

from __future__ import annotations

from urllib.parse import urljoin, urlparse, urlunparse

from selectolax.parser import HTMLParser
from loguru import logger


# Fragments, javascript links, and mail-to links are never useful
_SKIP_SCHEMES = {"javascript", "mailto", "tel", "data", ""}
_SKIP_EXTENSIONS = {
    ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp",
    ".mp4", ".mp3", ".zip", ".exe", ".dmg", ".apk",
    ".css", ".js", ".woff", ".woff2", ".ttf",
}


class LinkExtractor:
    """
    Extracts, normalises, and deduplicates all <a href> links from HTML.

    Usage:
        extractor = LinkExtractor()
        urls = extractor.extract(html, base_url="https://www.amazon.in/s?k=laptops")
    """

    def extract(self, html: str, base_url: str) -> list[str]:
        """
        Parse HTML and return a deduplicated list of absolute URLs
        found in <a href> attributes.
        """
        if not html:
            return []

        try:
            tree = HTMLParser(html)
        except Exception as exc:
            logger.warning(f"LinkExtractor: HTML parse error for {base_url}: {exc}")
            return []

        seen: set[str] = set()
        urls: list[str] = []

        for node in tree.css("a[href]"):
            href = node.attributes.get("href", "").strip()
            if not href:
                continue

            absolute = self._to_absolute(href, base_url)
            if absolute and absolute not in seen:
                seen.add(absolute)
                urls.append(absolute)

        logger.debug(f"LinkExtractor: found {len(urls)} unique links on {base_url}")
        return urls

    # ----------------------------------------------------------------- private

    def _to_absolute(self, href: str, base_url: str) -> str | None:
        """
        Convert a potentially relative href to an absolute URL.
        Returns None if the URL should be skipped.
        """
        # Resolve relative URLs
        try:
            absolute = urljoin(base_url, href)
        except Exception:
            return None

        parsed = urlparse(absolute)

        # Skip non-http(s) schemes
        if parsed.scheme in _SKIP_SCHEMES or parsed.scheme not in ("http", "https"):
            return None

        # Skip static asset extensions
        path_lower = parsed.path.lower().rstrip("/")
        if any(path_lower.endswith(ext) for ext in _SKIP_EXTENSIONS):
            return None

        # Normalise: drop fragment, lowercase scheme+host
        normalised = urlunparse((
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path,
            parsed.params,
            parsed.query,
            "",          # drop fragment — fragments are client-side only
        ))

        return normalised or None