"""
classifier/classified_queues.py — orchestrates classification + enqueue
for all URLs discovered on a single page.

This is the glue between the classifier and the queue manager:
  1. Extract links from the page.
  2. Classify each link.
  3. Pass the (url → PageType) map to QueueManager for gated enqueue.
"""

from __future__ import annotations

from loguru import logger

from classifier.page_classifier import PageClassifier
from classifier.page_types import PageType
from models import CrawlMeta


class ClassifiedQueues:
    """
    Stateless helper — one instance shared across the whole crawl.

    Usage (inside a Crawlee handler):
        page_type, enqueued = await cq.process(
            discovered_urls=links,
            parent_meta=meta,
            html=html_text,
            queue_manager=qm,
        )
    """

    def __init__(self) -> None:
        self._classifier = PageClassifier()

    async def process(
        self,
        discovered_urls: list[str],
        parent_meta: CrawlMeta,
        html: str,
        queue_manager,           # QueueManager — avoid circular import with string hint
    ) -> tuple[dict[str, PageType], int]:
        """
        Classify all discovered URLs and enqueue the valid ones.

        Returns:
            (classifications dict, number of URLs enqueued)
        """
        if not discovered_urls:
            return {}, 0

        # classify — URL rules first, DOM as fallback for UNKNOWN
        classifications = self._classifier.classify_batch(discovered_urls, html)

        # log a compact breakdown
        counts: dict[PageType, int] = {}
        for pt in classifications.values():
            counts[pt] = counts.get(pt, 0) + 1
        breakdown = ", ".join(f"{pt.value}={n}" for pt, n in sorted(counts.items(), key=lambda x: x[0].value))
        logger.debug(
            f"Classified {len(discovered_urls)} URLs from {parent_meta.seed_domain}: {breakdown}"
        )

        enqueued = await queue_manager.enqueue_discovered(
            urls=discovered_urls,
            parent_meta=parent_meta,
            page_types=classifications,
        )

        return classifications, enqueued