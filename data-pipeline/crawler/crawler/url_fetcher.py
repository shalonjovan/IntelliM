"""
crawler/url_fetcher.py — Crawlee request handler that processes every
fetched page:

  1. Extracts the CrawlMeta from user_data
  2. Extracts all links from the HTML
  3. Filters links through DomainScopeGuard
  4. Classifies + enqueues via ClassifiedQueues
  5. Follows pagination if on a category/listing page
  6. Records completion in ContextTracker

This handler is registered with the Crawlee router and called for
every page the crawler fetches.
"""

from __future__ import annotations

from crawlee.playwright_crawler import PlaywrightCrawlingContext
from crawlee.http_crawler import HttpCrawlingContext
from loguru import logger

from classifier.classified_queues import ClassifiedQueues
from classifier.page_types import PageType
from crawler.domain_scope_guard import DomainScopeGuard
from crawler.link_extractor import LinkExtractor
from crawler.pagination_discoverer import PaginationDiscoverer
from models import CrawlMeta
from queue.request_models import extract_meta, extract_page_type


class UrlFetcher:
    """
    Stateless handler factory.  Call build_handler() to get a coroutine
    compatible with crawlee's router.add_handler() / router.default_handler().

    One UrlFetcher instance is shared; its components are all thread-safe.
    """

    def __init__(
        self,
        scope_guard: DomainScopeGuard,
        classified_queues: ClassifiedQueues,
        queue_manager,               # QueueManager — forward ref avoids circular import
        context_tracker,             # ContextTracker
    ) -> None:
        self._scope_guard   = scope_guard
        self._cq            = classified_queues
        self._qm            = queue_manager
        self._tracker       = context_tracker
        self._link_extractor = LinkExtractor()
        self._paginator      = PaginationDiscoverer()

    # ------------------------------------------------------------------ public

    def get_http_handler(self):
        """Return an async handler for HttpCrawler contexts."""
        async def handler(context: HttpCrawlingContext) -> None:
            await self._handle(
                url=str(context.request.url),
                html=context.http_response.text,
                request=context.request,
                enqueue_links=context.enqueue_links,
            )
        return handler

    def get_playwright_handler(self):
        """Return an async handler for PlaywrightCrawler contexts."""
        async def handler(context: PlaywrightCrawlingContext) -> None:
            html = await context.page.content()
            await self._handle(
                url=str(context.request.url),
                html=html,
                request=context.request,
                enqueue_links=context.enqueue_links,
            )
        return handler

    # ----------------------------------------------------------------- private

    async def _handle(
        self,
        url: str,
        html: str,
        request,
        enqueue_links,           # Crawlee built-in helper (not used directly here)
    ) -> None:
        meta = extract_meta(request)
        page_type = extract_page_type(request)

        logger.info(
            f"[{meta.seed_category.value}] {page_type.value} | "
            f"depth={meta.depth} | {url}"
        )

        try:
            # 1. Extract all links
            all_links = self._link_extractor.extract(html, base_url=url)

            # 2. Scope-guard: keep only links on the same seed domain
            scoped_links = self._scope_guard.filter(all_links, meta)

            # 3. Classify + enqueue discovered links
            _, enqueued = await self._cq.process(
                discovered_urls=scoped_links,
                parent_meta=meta,
                html=html,
                queue_manager=self._qm,
            )

            # 4. Pagination — only follow on listing/category pages
            if page_type in (PageType.CATEGORY_PAGE, PageType.UNKNOWN):
                next_page = self._paginator.discover(html, url, meta)
                if next_page and self._scope_guard.is_allowed(next_page, meta):
                    added = await self._qm.enqueue_single(
                        url=next_page,
                        meta=meta,
                        page_type=PageType.CATEGORY_PAGE,
                    )
                    if added:
                        logger.debug(f"Pagination next page enqueued: {next_page}")

            # 5. Record completion
            self._tracker.record_complete(meta)

            logger.debug(
                f"Processed {url}: {len(scoped_links)} scoped links, "
                f"{enqueued} enqueued"
            )

        except Exception as exc:
            self._tracker.record_failed(meta)
            logger.error(f"Handler error on {url}: {exc}", exc_info=True)
            raise  # let Crawlee handle retry