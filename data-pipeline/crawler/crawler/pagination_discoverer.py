"""
crawler/pagination_discoverer.py — finds "next page" links within the
same domain context so the crawler follows listing pagination without
straying.

Supports:
  - Standard rel="next" links
  - Amazon ?page=N / &page=N query params
  - Flipkart ?page=N
  - Croma ?start=N / &start=N
  - Generic ?page=, ?p=, ?pg= patterns
"""

from __future__ import annotations

import re
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from selectolax.parser import HTMLParser
from loguru import logger

from models import CrawlMeta


# Query params that signal pagination
_PAGE_PARAMS = {"page", "p", "pg", "pageNumber", "start", "offset", "from"}

# URL path patterns that suggest a paginated listing
_PAGINATION_PATH_RE = re.compile(
    r"/(?:page|p)/(\d+)/?$", re.I
)


class PaginationDiscoverer:
    """
    Extracts the next-page URL from a fetched page, if one exists.

    Returns at most one next-page URL per call — the crawler will
    naturally follow the chain page-by-page.
    """

    def discover(self, html: str, current_url: str, meta: CrawlMeta) -> str | None:
        """
        Look for a next-page URL. Returns the absolute URL or None.

        Priority:
          1. <a rel="next"> link in HTML
          2. Recognised pagination query param incremented by 1
          3. Path-based /page/N pattern incremented by 1
        """
        if meta.is_at_max_depth:
            return None

        # 1. rel="next"
        next_url = self._rel_next(html, current_url)
        if next_url:
            logger.debug(f"Pagination (rel=next): {next_url}")
            return next_url

        # 2. Query-param based
        next_url = self._query_param_next(current_url)
        if next_url:
            logger.debug(f"Pagination (query param): {next_url}")
            return next_url

        # 3. Path-based /page/N
        next_url = self._path_next(current_url)
        if next_url:
            logger.debug(f"Pagination (path): {next_url}")
            return next_url

        return None

    # ----------------------------------------------------------------- private

    @staticmethod
    def _rel_next(html: str, base_url: str) -> str | None:
        if not html:
            return None
        try:
            tree = HTMLParser(html)
        except Exception:
            return None

        # <link rel="next" href="..."> or <a rel="next" href="...">
        for selector in ('link[rel="next"]', 'a[rel="next"]'):
            node = tree.css_first(selector)
            if node:
                href = node.attributes.get("href", "").strip()
                if href:
                    return urljoin(base_url, href)
        return None

    @staticmethod
    def _query_param_next(url: str) -> str | None:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=True)

        for param in _PAGE_PARAMS:
            if param in qs:
                try:
                    current_page = int(qs[param][0])
                except (ValueError, IndexError):
                    continue
                qs[param] = [str(current_page + 1)]
                new_query = urlencode(
                    {k: v[0] for k, v in qs.items()}, doseq=False
                )
                return urlunparse(parsed._replace(query=new_query))

        return None

    @staticmethod
    def _path_next(url: str) -> str | None:
        parsed = urlparse(url)
        m = _PAGINATION_PATH_RE.search(parsed.path)
        if m:
            current_page = int(m.group(1))
            new_path = _PAGINATION_PATH_RE.sub(
                f"/page/{current_page + 1}/", parsed.path
            )
            return urlunparse(parsed._replace(path=new_path))
        return None