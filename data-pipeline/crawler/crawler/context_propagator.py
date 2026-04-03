"""
crawler/context_propagator.py — enriches child CrawlMeta objects before
they are enqueued, keeping seed context alive across the full crawl depth.

Responsibilities:
  - Increment depth counter
  - Carry forward seed_category, seed_domain, origin_url
  - Attach product_hint when a URL matches a known product keyword
  - Provide a clean child() factory that callers use instead of
    mutating CrawlMeta directly
"""

from __future__ import annotations

import re
from urllib.parse import urlparse

from loguru import logger

from models import CrawlMeta, SeedConfig, SeedProduct


class ContextPropagator:
    """
    Builds child CrawlMeta objects for discovered URLs.

    Usage:
        propagator = ContextPropagator(seed_configs)
        child_meta = propagator.propagate(parent_meta, child_url)
    """

    def __init__(self, seed_configs: list[SeedConfig]) -> None:
        # Pre-index: keyword → model_id for fast product hinting
        self._keyword_index: list[tuple[re.Pattern[str], str]] = []
        for cfg in seed_configs:
            for product in cfg.products:
                for kw in product.search_keywords:
                    pattern = re.compile(re.escape(kw), re.I)
                    self._keyword_index.append((pattern, product.model_id))

        logger.debug(
            f"ContextPropagator: indexed {len(self._keyword_index)} product keywords"
        )

    # ------------------------------------------------------------------ public

    def propagate(self, parent_meta: CrawlMeta, child_url: str) -> CrawlMeta:
        """
        Create a child CrawlMeta from the parent, with:
          - depth incremented by 1
          - parent_url set to the current page's URL (origin_url at depth 0)
          - product_hint filled in if the URL matches a known product keyword
        """
        hint = self._detect_product_hint(child_url, parent_meta.product_hint)

        child = CrawlMeta(
            seed_category=parent_meta.seed_category,
            seed_domain=self._domain_of(child_url) or parent_meta.seed_domain,
            origin_url=parent_meta.origin_url,
            depth=parent_meta.depth + 1,
            max_depth=parent_meta.max_depth,
            parent_url=parent_meta.origin_url if parent_meta.depth == 0
                       else parent_meta.parent_url,
            product_hint=hint,
        )
        return child

    # ----------------------------------------------------------------- private

    def _detect_product_hint(
        self,
        url: str,
        existing_hint: str | None,
    ) -> str | None:
        """
        If the URL contains a product keyword, return the model_id.
        Keep existing hint if already set (don't overwrite with a less specific one).
        """
        if existing_hint:
            return existing_hint

        for pattern, model_id in self._keyword_index:
            if pattern.search(url):
                logger.debug(f"Product hint detected: {model_id} | {url}")
                return model_id

        return None

    @staticmethod
    def _domain_of(url: str) -> str | None:
        try:
            return urlparse(url).netloc or None
        except Exception:
            return None