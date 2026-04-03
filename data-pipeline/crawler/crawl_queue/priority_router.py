"""
queue/priority_router.py — decides which Crawlee label (sub-queue) a
classified URL should be enqueued into, and applies any priority rules.

Crawlee-python uses string labels to route requests to registered handlers.
Each PageType maps to its own label so downstream handlers are decoupled.
"""

from __future__ import annotations

from crawlee import Request
from loguru import logger

from classifier.page_types import PageType
from models import CrawlMeta
from crawl_queue.context_tracker import ContextTracker
from crawl_queue.request_models import make_request


# Priority order (lower index = higher priority).
# Crawlee does not natively support numeric priority in the open-source
# version, so we emulate it by controlling enqueue order — high-priority
# requests are added first so they are picked up sooner.
_PRIORITY_ORDER: list[PageType] = [
    PageType.PRODUCT_PAGE,
    PageType.REVIEW_PAGE,
    PageType.CATEGORY_PAGE,
    PageType.AD_CREATIVE,
    PageType.TREND_DATA,
    PageType.UNKNOWN,
]


class PriorityRouter:
    """
    Takes a (url, meta, page_type) triple and decides:
      1. Whether the URL should be enqueued at all (domain cap, depth, dedup).
      2. Which label to attach so Crawlee routes it to the right handler.
    """

    def __init__(self, tracker: ContextTracker) -> None:
        self._tracker = tracker

    def should_enqueue(self, url: str, meta: CrawlMeta) -> bool:
        """Gate checks before any URL hits the queue."""
        if meta.is_at_max_depth:
            logger.debug(f"Depth cap reached — skipping {url}")
            return False

        if not self._tracker.can_enqueue(meta.seed_domain):
            logger.debug(f"Domain cap reached for {meta.seed_domain} — skipping {url}")
            return False

        if not self._tracker.mark_seen(url):
            logger.debug(f"Already seen — skipping {url}")
            return False

        return True

    def build_request(
        self,
        url: str,
        meta: CrawlMeta,
        page_type: PageType,
    ) -> Request | None:
        """
        Run gate checks then build a Crawlee Request with the correct label.
        Returns None if the URL should not be enqueued.
        """
        child_meta = meta.child(url)

        if not self.should_enqueue(url, child_meta):
            return None

        self._tracker.record_enqueue(child_meta)

        req = make_request(
            url=url,
            meta=child_meta,
            page_type=page_type,
        )

        logger.debug(
            f"Routing {url!r} → {page_type.queue_name} "
            f"(depth {child_meta.depth}, domain {child_meta.seed_domain})"
        )
        return req

    @staticmethod
    def priority_sort(requests: list[Request]) -> list[Request]:
        """
        Sort a batch of requests by page type priority so high-value
        pages are processed before generic category pages.
        """
        def _key(r: Request) -> int:
            pt_val = r.user_data.get("page_type", PageType.UNKNOWN.value)
            try:
                pt = PageType(pt_val)
            except ValueError:
                pt = PageType.UNKNOWN
            try:
                return _PRIORITY_ORDER.index(pt)
            except ValueError:
                return len(_PRIORITY_ORDER)

        return sorted(requests, key=_key)