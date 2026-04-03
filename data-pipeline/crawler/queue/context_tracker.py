"""
queue/context_tracker.py — tracks per-domain request counts and depth
budgets so the crawler never strays or over-crawls a single domain.
"""

from __future__ import annotations

import threading
from collections import defaultdict

from loguru import logger

from models import CrawlMeta


class ContextTracker:
    """
    Thread-safe in-memory tracker that records:
      - how many requests have been enqueued per domain
      - how many requests have been completed per domain
      - which URLs have already been seen (dedup)

    One instance is shared across the whole crawl run via pipeline.py.
    """

    def __init__(self, max_requests_per_domain: int = 500) -> None:
        self._lock = threading.Lock()
        self._max_per_domain = max_requests_per_domain

        self._enqueued: dict[str, int] = defaultdict(int)
        self._completed: dict[str, int] = defaultdict(int)
        self._failed: dict[str, int] = defaultdict(int)
        self._seen_urls: set[str] = set()

    # ------------------------------------------------------------------ public

    def is_seen(self, url: str) -> bool:
        with self._lock:
            return url in self._seen_urls

    def mark_seen(self, url: str) -> bool:
        """
        Mark url as seen. Returns True if it was new (should be enqueued),
        False if it was already seen (skip it).
        """
        with self._lock:
            if url in self._seen_urls:
                return False
            self._seen_urls.add(url)
            return True

    def can_enqueue(self, domain: str) -> bool:
        """Return True if the domain has not yet hit its request cap."""
        with self._lock:
            return self._enqueued[domain] < self._max_per_domain

    def record_enqueue(self, meta: CrawlMeta) -> None:
        with self._lock:
            self._enqueued[meta.seed_domain] += 1

    def record_complete(self, meta: CrawlMeta) -> None:
        with self._lock:
            self._completed[meta.seed_domain] += 1

    def record_failed(self, meta: CrawlMeta) -> None:
        with self._lock:
            self._failed[meta.seed_domain] += 1

    # --------------------------------------------------------------- reporting

    def summary(self) -> dict[str, dict[str, int]]:
        with self._lock:
            domains = set(self._enqueued) | set(self._completed) | set(self._failed)
            return {
                d: {
                    "enqueued":  self._enqueued[d],
                    "completed": self._completed[d],
                    "failed":    self._failed[d],
                }
                for d in sorted(domains)
            }

    def log_summary(self) -> None:
        for domain, counts in self.summary().items():
            logger.info(
                f"[{domain}] enqueued={counts['enqueued']} "
                f"completed={counts['completed']} "
                f"failed={counts['failed']}"
            )