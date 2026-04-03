"""
seeds/base_seed.py — loads and validates a single seed JSON file.
"""

from __future__ import annotations

import json
from pathlib import Path

from loguru import logger
from pydantic import ValidationError

from models import SeedConfig


def load_seed_file(path: Path) -> SeedConfig | None:
    """
    Load one seed JSON file, validate it against SeedConfig, and return it.
    Returns None if the file is invalid (logs the error).
    """
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        config = SeedConfig.model_validate(raw)
        logger.debug(f"Loaded seed '{config.category}' — {len(config.products)} products, "
                     f"{len(config.start_urls)} start URLs")
        return config
    except (json.JSONDecodeError, ValidationError) as exc:
        logger.error(f"Failed to load seed file {path}: {exc}")
        return None