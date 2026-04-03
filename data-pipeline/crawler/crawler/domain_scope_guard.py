"""
crawler/domain_scope_guard.py — enforces that all discovered URLs belong
to the same seed domain (or an explicitly allowed subdomain of it).

This is the primary mechanism that keeps the crawler from straying.
"""

from __future__ import annotations

from urllib.parse import urlparse

import tldextract
from loguru import logger

from models import CrawlMeta, SeedConfig


class DomainScopeGuard:
    """
    Validates discovered URLs against the seed domain allow-list.

    Rules:
      - The URL's registered domain (e.g. amazon.in) must match one of the
        seed config's declared domains.
      - Subdomains are allowed (e.g. www.amazon.in, m.amazon.in).
      - Completely different domains are rejected.
      - Non-HTTP(S) schemes are always rejected.
    """

    def __init__(self, seed_configs: list[SeedConfig]) -> None:
        # Build a flat set of allowed registered domains from all seed configs
        self._allowed: set[str] = set()
        for cfg in seed_configs:
            for domain in cfg.domains:
                registered = self._registered_domain(domain)
                if registered:
                    self._allowed.add(registered)

        logger.debug(f"DomainScopeGuard: allowed domains = {sorted(self._allowed)}")

    # ------------------------------------------------------------------ public

    def is_allowed(self, url: str, meta: CrawlMeta | None = None) -> bool:
        """
        Return True if the URL's domain is within the allowed seed domains.
        If meta is provided, also checks against the specific seed domain.
        """
        parsed = urlparse(url)

        if parsed.scheme not in ("http", "https"):
            return False

        url_registered = self._registered_domain(parsed.netloc)
        if not url_registered:
            return False

        # Primary check: against global allow-list
        if url_registered not in self._allowed:
            logger.debug(f"Out-of-scope domain blocked: {url_registered} | {url}")
            return False

        # Secondary check (optional): must also match the specific seed domain
        if meta is not None:
            seed_registered = self._registered_domain(meta.seed_domain)
            if seed_registered and url_registered != seed_registered:
                logger.debug(
                    f"Cross-seed domain blocked: {url_registered} != {seed_registered} | {url}"
                )
                return False

        return True

    def filter(self, urls: list[str], meta: CrawlMeta | None = None) -> list[str]:
        """Filter a list of URLs, returning only in-scope ones."""
        allowed = [u for u in urls if self.is_allowed(u, meta)]
        rejected = len(urls) - len(allowed)
        if rejected:
            logger.debug(f"DomainScopeGuard: blocked {rejected}/{len(urls)} out-of-scope URLs")
        return allowed

    # ----------------------------------------------------------------- private

    @staticmethod
    def _registered_domain(host: str) -> str:
        """
        Extract the registered domain (e.g. amazon.in) from a hostname or URL.
        Uses tldextract to handle multi-part TLDs correctly.
        """
        ext = tldextract.extract(host)
        if ext.domain and ext.suffix:
            return f"{ext.domain}.{ext.suffix}"
        return ""