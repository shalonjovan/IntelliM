"""
pipeline.py — wires all four layers together and runs the crawl.

Layer flow:
  1. Seeds      → load JSON configs, build initial Request objects
  2. Queue      → prime Crawlee RequestQueue, set up context tracking
  3. Crawler    → fetch pages, extract links, apply scope guard
  4. Classifier → classify discovered links, route to typed sub-queues
"""

from __future__ import annotations

from loguru import logger

from classifier.classified_queues import ClassifiedQueues
from config.settings import settings
from crawler.base_crawler import build_crawler
from crawler.context_propagator import ContextPropagator
from crawler.domain_scope_guard import DomainScopeGuard
from crawler.url_fetcher import UrlFetcher
from models import SeedConfig
from crawl_queue.context_tracker import ContextTracker
from crawl_queue.queue_manager import QueueManager
from seeds.seed_loader import build_initial_requests, load_all_seeds


async def run(seed_configs: list[SeedConfig] | None = None) -> None:
    """
    Full pipeline entry point.

    Args:
        seed_configs: Optional pre-loaded configs (useful for testing).
                      If None, all JSONs in settings.seed_configs_dir are loaded.
    """

    # ── Layer 1: Seeds ───────────────────────────────────────────────────────
    if seed_configs is None:
        seed_configs = load_all_seeds()

    if not seed_configs:
        logger.error("No seed configs loaded — aborting.")
        return

    logger.info(
        f"Pipeline starting | {len(seed_configs)} categories | "
        f"storage: {settings.storage_dir}"
    )

    initial_requests = build_initial_requests(seed_configs)

    # ── Layer 2: Queue ───────────────────────────────────────────────────────
    tracker = ContextTracker(
        max_requests_per_domain=settings.max_requests_per_domain
    )
    queue_manager = await QueueManager.create(tracker)
    await queue_manager.enqueue_seed_requests(initial_requests)

    # ── Layer 3 + 4: Crawler + Classifier ───────────────────────────────────
    scope_guard   = DomainScopeGuard(seed_configs)
    propagator    = ContextPropagator(seed_configs)   # used by QueueManager internally
    classified_qs = ClassifiedQueues()

    fetcher = UrlFetcher(
        scope_guard=scope_guard,
        classified_queues=classified_qs,
        queue_manager=queue_manager,
        context_tracker=tracker,
    )

    crawler = build_crawler(fetcher)

    # ── Run ──────────────────────────────────────────────────────────────────
    logger.info("Crawl starting …")
    await crawler.run(
        requests=None,   # queue is already primed above
    )

    # ── Summary ──────────────────────────────────────────────────────────────
    logger.info("Crawl complete. Per-domain summary:")
    tracker.log_summary()