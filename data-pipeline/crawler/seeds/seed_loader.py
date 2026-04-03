"""
seeds/seed_loader.py — reads all seed JSON files, validates them,
and builds the initial list of crawlee Request objects to prime the queue.
"""

from __future__ import annotations

from pathlib import Path

from crawlee import Request
from loguru import logger

from config.settings import settings
from models import CrawlMeta, SeedConfig
from seeds.base_seed import load_seed_file


def load_all_seeds(seed_dir: Path | None = None) -> list[SeedConfig]:
    """
    Scan the seed_configs directory, load every *.json file, and return
    the list of valid SeedConfig objects.
    """
    seed_dir = seed_dir or settings.seed_configs_dir
    if not seed_dir.exists():
        raise FileNotFoundError(f"Seed config directory not found: {seed_dir}")

    json_files = sorted(seed_dir.glob("*.json"))
    if not json_files:
        raise ValueError(f"No seed JSON files found in {seed_dir}")

    configs: list[SeedConfig] = []
    for f in json_files:
        cfg = load_seed_file(f)
        if cfg is not None:
            configs.append(cfg)

    logger.info(f"Loaded {len(configs)}/{len(json_files)} seed files from {seed_dir}")
    return configs


def build_initial_requests(configs: list[SeedConfig]) -> list[Request]:
    """
    Convert all seed start_urls into Crawlee Request objects,
    embedding a CrawlMeta payload in user_data so every downstream
    handler knows which seed, domain and depth this request belongs to.
    """
    requests: list[Request] = []

    for cfg in configs:
        effective_max_depth = min(cfg.max_depth, settings.global_max_depth)

        for url in cfg.start_urls:
            # Derive the seed domain from the URL itself
            from urllib.parse import urlparse
            domain = urlparse(url).netloc

            meta = CrawlMeta(
                seed_category=cfg.category,
                seed_domain=domain,
                origin_url=url,
                depth=0,
                max_depth=effective_max_depth,
                parent_url=None,
                product_hint=None,
            )

            req = Request.from_url(
                url=url,
                user_data=meta.to_dict(),
                label="SEED",           # Crawlee uses labels to route to handlers
            )
            requests.append(req)

    logger.info(f"Built {len(requests)} initial seed requests across "
                f"{len(configs)} categories")
    return requests