"""
classifier/page_classifier.py — classifies a URL (and optionally its HTML)
into a PageType by running URL rules first, then DOM rules as a fallback.
"""

from __future__ import annotations

from loguru import logger

from classifier.classifier_rules import get_dom_rules, get_url_rules
from classifier.page_types import PageType


class PageClassifier:
    """
    Two-stage classifier:
      1. URL rules  — fast, no HTML needed, runs on every discovered link.
      2. DOM rules  — slower, requires fetched HTML, used as fallback.

    Usage:
        classifier = PageClassifier()

        # Before fetch (URL only):
        page_type = classifier.classify_url(url)

        # After fetch (URL + HTML for fallback):
        page_type = classifier.classify(url, html)
    """

    def __init__(self) -> None:
        self._url_rules = get_url_rules()
        self._dom_rules = get_dom_rules()

    # ------------------------------------------------------------------ public

    def classify_url(self, url: str) -> PageType:
        """
        Classify by URL pattern alone.
        Returns UNKNOWN if no rule matches — caller should fetch and use classify().
        """
        for rule in self._url_rules:
            if rule.matches(url):
                logger.debug(f"URL rule matched '{rule.description}' → {rule.page_type} | {url}")
                return rule.page_type
        return PageType.UNKNOWN

    def classify(self, url: str, html: str = "") -> PageType:
        """
        Full classification: URL rules first, DOM rules as fallback.
        Always returns a PageType (never raises).
        """
        page_type = self.classify_url(url)
        if page_type is not PageType.UNKNOWN:
            return page_type

        if html:
            for rule in self._dom_rules:
                if rule.matches(html):
                    logger.debug(
                        f"DOM rule matched '{rule.description}' → {rule.page_type} | {url}"
                    )
                    return rule.page_type

        logger.debug(f"No rule matched — UNKNOWN | {url}")
        return PageType.UNKNOWN

    def classify_batch(
        self,
        urls: list[str],
        html: str = "",
    ) -> dict[str, PageType]:
        """
        Classify multiple URLs at once (URL rules only — fast path).
        Optionally pass the parent page HTML so DOM rules can be used
        for URLs that don't match any URL pattern.
        """
        return {url: self.classify(url, html) for url in urls}