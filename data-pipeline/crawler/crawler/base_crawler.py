"""
crawler/base_crawler.py — builds and configures the Crawlee crawler instance.

Supports two backends:
  - HttpCrawler  (default) — fast, uses httpx, works for most e-commerce pages
  - PlaywrightCrawler      — for JS-heavy pages, enabled via settings.use_playwright

The router is configured here with:
  - A default handler (catches all labels not explicitly registered)
  - Per-label handlers for each PageType so we can add specialised
    extraction logic later (e.g. scraping price from PRODUCT_PAGE)
    without changing the routing layer.
"""

from __future__ import annotations

import random

from crawlee.crawlers import HttpCrawler, HttpCrawlingContext
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.router import Router
from crawlee import ConcurrencySettings
from loguru import logger

from classifier.page_types import PageType
from config.settings import settings
from crawler.url_fetcher import UrlFetcher


# Realistic browser User-Agent pool to avoid trivial bot detection
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",

    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",

    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",

    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
    "Gecko/20100101 Firefox/125.0",
]


def _random_ua() -> str:
    return random.choice(_USER_AGENTS)


def build_http_crawler(fetcher: UrlFetcher) -> HttpCrawler:
    """Build and configure an HttpCrawler with per-label routing."""
    router: Router[HttpCrawlingContext] = Router()
    handler = fetcher.get_http_handler()

    # Register a handler for every PageType label
    for pt in PageType:
        router.handler(pt.queue_name)(handler)

    # Default handler catches seed requests and anything unclassified
    router.default_handler(handler)

    crawler = HttpCrawler(
        request_handler=router,
        concurrency_settings=ConcurrencySettings(
            max_concurrency=settings.max_concurrency,
            desired_concurrency=settings.max_concurrency,
        ),
        max_request_retries=settings.max_retries,
        request_handler_timeout=_timeout(),
        additional_http_error_status_codes=[403, 429],
    )

    logger.info(
        f"HttpCrawler built | concurrency={settings.max_concurrency} "
        f"retries={settings.max_retries}"
    )
    return crawler


def build_playwright_crawler(fetcher: UrlFetcher) -> PlaywrightCrawler:
    """Build and configure a PlaywrightCrawler for JS-heavy sites."""
    router: Router[PlaywrightCrawlingContext] = Router()
    handler = fetcher.get_playwright_handler()

    for pt in PageType:
        router.add_handler(pt.queue_name)(handler)

    router.default_handler(handler)

    crawler = PlaywrightCrawler(
        request_handler=router,
        concurrency_settings=ConcurrencySettings(
            max_concurrency=settings.max_concurrency,
            desired_concurrency=settings.max_concurrency,
        ),
        max_request_retries=settings.max_retries,
        request_handler_timeout=_timeout(),
        headless=settings.headless,
        browser_type=settings.browser_type,
    )

    logger.info(
        f"PlaywrightCrawler built | concurrency={settings.max_concurrency} "
        f"browser={settings.browser_type} headless={settings.headless}"
    )
    return crawler


def build_crawler(fetcher: UrlFetcher) -> HttpCrawler | PlaywrightCrawler:
    """Select the right crawler based on settings."""
    if settings.use_playwright:
        return build_playwright_crawler(fetcher)
    return build_http_crawler(fetcher)


def _timeout():
    """Convert settings timeout to a timedelta for Crawlee."""
    from datetime import timedelta
    return timedelta(seconds=settings.request_timeout_secs)