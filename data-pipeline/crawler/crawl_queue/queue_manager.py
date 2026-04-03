"""
queue/queue_manager.py — thin wrapper around Crawlee's RequestQueue that
provides named logical sub-queues and batch-enqueue helpers.

Crawlee-python uses a single RequestQueue under the hood; logical separation
is achieved through the Request `label` field which routes to registered
handlers in the router. This manager keeps the enqueue logic in one place.
"""

from __future__ import annotations

from crawlee import Request
from crawlee.storages import RequestQueue
from loguru import logger

from classifier.page_types import PageType
from crawl_queue.context_tracker import ContextTracker
from crawl_queue.priority_router import PriorityRouter
from models import CrawlMeta


class QueueManager:
    """
    Central point for adding requests to Crawlee's queue.

    Usage:
        qm = await QueueManager.create(tracker)
        await qm.enqueue_seed_requests(initial_requests)
        await qm.enqueue_discovered(urls, parent_meta, page_types)
    """

    def __init__(
        self,
        request_queue: RequestQueue,
        tracker: ContextTracker,
        router: PriorityRouter,
    ) -> None:
        self._queue   = request_queue
        self._tracker = tracker
        self._router  = router

    @classmethod
    async def create(cls, tracker: ContextTracker) -> "QueueManager":
        """Async factory — opens (or re-opens) the default Crawlee RequestQueue."""
        rq = await RequestQueue.open()
        router = PriorityRouter(tracker)
        logger.info("QueueManager: RequestQueue opened")
        return cls(rq, tracker, router)

    # ------------------------------------------------------------------ public

    async def enqueue_seed_requests(self, requests: list[Request]) -> int:
        """
        Prime the queue with the initial seed requests.
        These bypass the normal gate checks (depth=0, always new).
        Returns the number of requests actually added.
        """
        added = 0
        for req in requests:
            result = await self._queue.add_request(req)
            if not result.was_already_present:
                added += 1
                # mark seed URLs as seen so they aren't re-enqueued later
                self._tracker.mark_seen(req.url)
        logger.info(f"Seeded queue with {added} initial requests")
        return added

    async def enqueue_discovered(
        self,
        urls: list[str],
        parent_meta: CrawlMeta,
        page_types: dict[str, PageType],
    ) -> int:
        """
        Enqueue a batch of URLs discovered during crawling.

        Args:
            urls:       discovered URLs to consider
            parent_meta: CrawlMeta from the page that discovered them
            page_types:  mapping of url → PageType (from classifier)

        Returns the number of URLs actually enqueued.
        """
        requests: list[Request] = []

        for url in urls:
            pt = page_types.get(url, PageType.UNKNOWN)
            req = self._router.build_request(url, parent_meta, pt)
            if req is not None:
                requests.append(req)

        # Sort by priority before adding to the queue
        requests = PriorityRouter.priority_sort(requests)

        added = 0
        for req in requests:
            result = await self._queue.add_request(req)
            if not result.was_already_present:
                added += 1

        if added:
            logger.debug(
                f"Enqueued {added}/{len(urls)} discovered URLs "
                f"(domain: {parent_meta.seed_domain})"
            )
        return added

    async def enqueue_single(
        self,
        url: str,
        meta: CrawlMeta,
        page_type: PageType = PageType.UNKNOWN,
    ) -> bool:
        """Convenience method to enqueue a single URL. Returns True if added."""
        req = self._router.build_request(url, meta, page_type)
        if req is None:
            return False
        result = await self._queue.add_request(req)
        return not result.was_already_present

    @property
    def request_queue(self) -> RequestQueue:
        return self._queue