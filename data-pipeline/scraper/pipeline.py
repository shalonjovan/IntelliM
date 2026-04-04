"""
pipeline.py — Scraper pipeline orchestrator.

Reads discovered URLs from the Crawlee request queue, re-fetches each
page's HTML using httpx, then runs: parse → clean → classify → normalize
→ write JSONL.

Why re-fetch?
  Crawlee's request_queues/*.json files store URL metadata and crawl
  context (user_data), but NOT the fetched HTML body. The HTML must
  be re-fetched at scrape time.

Usage:
    from scraper.pipeline import ScraperPipeline, ScraperConfig
    cfg = ScraperConfig(storage_dir=Path("../crawler/storage"), output_dir=Path("./output"))
    await ScraperPipeline(cfg).run()
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from loguru import logger

from classifier.content_classifier import ContentClassifier
from models import ContentType
from normalizer.ad_normalizer import ad_normalizer
from normalizer.product_normalizer import product_normalizer
from normalizer.review_normalizer import review_normalizer
from normalizer.trend_normalizer import trend_normalizer
from parser.ad_parser import AdParser
from parser.html_parser import HtmlParser
from parser.product_parser import ProductParser
from parser.review_parser import ReviewParser
from parser.trend_parser import TrendParser
from storage.jsonl_writer import JsonlWriter


# Realistic UA pool (same as crawler)
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
    "Gecko/20100101 Firefox/125.0",
]


@dataclass
class ScraperConfig:
    """Configuration for a single scraper pipeline run."""
    storage_dir: Path              # path to crawler storage/ directory
    output_dir: Path               # where JSONL output files are written
    categories: list[str] = field(default_factory=list)
    max_workers: int = 4           # parallel fetch+parse workers
    dry_run: bool = False          # discover & log only, no fetch or write
    request_delay: float = 1.0    # seconds between requests per domain
    request_timeout: int = 20     # httpx timeout in seconds


class ScraperPipeline:
    """
    Scraper pipeline: reads queue entries → fetches HTML → parses →
    normalizes → writes JSONL.
    """

    def __init__(self, config: ScraperConfig) -> None:
        self.cfg = config
        self._classifier = ContentClassifier()
        self._parsers: dict[str, Any] = {
            "product_page":  ProductParser(),
            "review_page":   ReviewParser(),
            "ad_creative":   AdParser(),
            "trend_data":    TrendParser(),
            "category_page": HtmlParser(),
            "unknown":       HtmlParser(),
        }
        # Per-domain rate-limit tracking  {domain: last_request_time}
        self._domain_last: dict[str, float] = {}
        self._rate_lock = asyncio.Lock()

    # ------------------------------------------------------------------ public

    async def run(self) -> dict[str, int]:
        """Run the full scraper pipeline."""
        pages = self._discover_queue_entries()
        logger.info(f"Discovered {len(pages)} queue entries.")

        if not pages:
            logger.warning(
                "No queue entries found. "
                f"Looked in: {self.cfg.storage_dir / 'request_queues'}"
            )
            return {}

        if self.cfg.dry_run:
            logger.info("Dry run — showing sample, skipping fetch+write.")
            for p in pages[:10]:
                logger.info(
                    f"  [{p['seed_category']}] {p['page_type_hint']:15s} | {p['url']}"
                )
            return {"discovered": len(pages)}

        async with httpx.AsyncClient(
            timeout=self.cfg.request_timeout,
            follow_redirects=True,
            headers={"Accept-Language": "en-IN,en;q=0.9"},
        ) as client:
            with JsonlWriter(self.cfg.output_dir) as writer:
                sem = asyncio.Semaphore(self.cfg.max_workers)
                tasks = [
                    self._process_entry(entry, client, writer, sem)
                    for entry in pages
                ]
                await asyncio.gather(*tasks)
                summary = writer.summary()

        logger.info("Scraper pipeline complete. Output summary:")
        for fname, count in sorted(summary.items()):
            logger.info(f"  {fname}: {count} records")
        return summary

    # ------------------------------------------------------------------ private

    def _discover_queue_entries(self) -> list[dict[str, Any]]:
        """
        Read all Crawlee request-queue JSON files and return a flat list
        of page dicts ready for scraping.

        Queue file format (Crawlee ≥ 0.6):
          {
            "url": "https://...",
            "user_data": {
              "label": "queue_product_page",
              "page_type": "product_page",
              "seed_category": "smartphones",
              "seed_domain": "www.flipkart.com",
              ...
            },
            "handled_at": "2026-04-03T12:22:24.938400"  ← only if crawled
          }
        """
        entries: list[dict[str, Any]] = []
        rq_dir = self.cfg.storage_dir / "request_queues"

        if not rq_dir.exists():
            logger.warning(f"Request queues directory not found: {rq_dir}")
            return entries

        cat_set = set(self.cfg.categories) if self.cfg.categories else None

        for jf in rq_dir.rglob("*.json"):
            try:
                raw = json.loads(jf.read_text(encoding="utf-8"))
            except Exception as exc:
                logger.debug(f"Skip bad JSON {jf.name}: {exc}")
                continue

            url = raw.get("url", "")
            if not url:
                continue

            # user_data key can be snake_case or camelCase depending on version
            ud: dict = raw.get("user_data") or raw.get("userData") or {}

            page_type = (
                ud.get("page_type")
                or ud.get("label", "").replace("queue_", "")
                or "unknown"
            )
            seed_category = ud.get("seed_category", "unknown")
            seed_domain   = ud.get("seed_domain", urlparse(url).netloc)

            # Category filter
            if cat_set and seed_category not in cat_set:
                continue

            entries.append({
                "url":            url,
                "page_type_hint": page_type,
                "seed_category":  seed_category,
                "seed_domain":    seed_domain,
                "depth":          ud.get("depth", 0),
                "crawled_at":     ud.get("crawled_at", ""),
            })

        logger.info(
            f"Loaded {len(entries)} queue entries from "
            f"{self.cfg.storage_dir / 'request_queues'}"
        )
        return entries

    async def _process_entry(
        self,
        entry: dict,
        client: httpx.AsyncClient,
        writer: JsonlWriter,
        sem: asyncio.Semaphore,
    ) -> None:
        async with sem:
            url = entry["url"]
            try:
                html = await self._fetch(url, entry["seed_domain"], client)
                if not html:
                    return
                await asyncio.get_event_loop().run_in_executor(
                    None, self._parse_and_write, entry, html, writer
                )
            except Exception as exc:
                logger.error(f"Error processing {url}: {exc}")

    async def _fetch(
        self, url: str, domain: str, client: httpx.AsyncClient
    ) -> str:
        """Fetch a URL with per-domain rate limiting."""
        # Rate limit per domain
        async with self._rate_lock:
            last = self._domain_last.get(domain, 0)
            wait = self.cfg.request_delay - (time.monotonic() - last)
            if wait > 0:
                await asyncio.sleep(wait)
            self._domain_last[domain] = time.monotonic()

        ua = random.choice(_USER_AGENTS)
        try:
            resp = await client.get(url, headers={"User-Agent": ua})
            if resp.status_code == 200:
                return resp.text
            logger.debug(f"HTTP {resp.status_code} for {url}")
        except httpx.TimeoutException:
            logger.debug(f"Timeout: {url}")
        except Exception as exc:
            logger.debug(f"Fetch error {url}: {exc}")
        return ""

    def _parse_and_write(
        self, entry: dict, html: str, writer: JsonlWriter
    ) -> None:
        url           = entry["url"]
        page_hint     = entry["page_type_hint"]
        seed_category = entry["seed_category"]
        seed_domain   = entry["seed_domain"]

        parser = self._parsers.get(page_hint, self._parsers["unknown"])
        fields = parser.parse(html, url)
        fields["seed_category"] = seed_category
        fields["seed_domain"]   = seed_domain

        content_type = self._classifier.classify(
            detected_type=fields.get("detected_type", ""),
            page_type_hint=page_hint,
            parsed_fields=fields,
        )
        logger.debug(f"[{seed_category}] {content_type.value} | {url}")
        self._normalize_and_write(content_type, fields, url, seed_category, writer)

    def _normalize_and_write(
        self,
        content_type: ContentType,
        fields: dict,
        url: str,
        seed_category: str,
        writer: JsonlWriter,
    ) -> None:
        try:
            if content_type == ContentType.PRODUCT:
                record = product_normalizer.normalize(fields, url, seed_category)
                writer.write(ContentType.PRODUCT, record.model_dump(mode="json"))
            elif content_type == ContentType.REVIEW:
                records = review_normalizer.normalize_many(fields, url, seed_category)
                writer.write_many(ContentType.REVIEW, [r.model_dump(mode="json") for r in records])
            elif content_type == ContentType.AD:
                record = ad_normalizer.normalize(fields, url, seed_category)
                writer.write(ContentType.AD, record.model_dump(mode="json"))
            elif content_type == ContentType.TREND:
                records = trend_normalizer.normalize_many(fields, url, seed_category)
                writer.write_many(ContentType.TREND, [r.model_dump(mode="json") for r in records])
            else:
                writer.write(ContentType.OTHER, {
                    "content_type": content_type.value,
                    "url": url,
                    "seed_category": seed_category,
                    "page_title": fields.get("page_title", ""),
                })
        except Exception as exc:
            logger.error(f"Normalization failed for {url}: {exc}", exc_info=True)
