"""
storage/jsonl_writer.py — Appends NormalizedRecord dicts to typed JSONL files.

Output layout:
  <output_dir>/
    products.jsonl
    reviews.jsonl
    ads.jsonl
    trends.jsonl
    unclassified.jsonl
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import orjson
from loguru import logger

from models import ContentType

_FILE_MAP: dict[ContentType, str] = {
    ContentType.PRODUCT:  "products.jsonl",
    ContentType.REVIEW:   "reviews.jsonl",
    ContentType.AD:       "ads.jsonl",
    ContentType.TREND:    "trends.jsonl",
    ContentType.CATEGORY: "unclassified.jsonl",
    ContentType.OTHER:    "unclassified.jsonl",
}


class JsonlWriter:
    """
    Thread-safe JSONL writer. One file handle per content type, opened lazily.
    Call close() when done.
    """

    def __init__(self, output_dir: Path) -> None:
        self._dir = output_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._handles: dict[str, Any] = {}
        self._lock = threading.Lock()
        self._counts: dict[str, int] = {}

    def write(self, content_type: ContentType, record: dict) -> None:
        """Serialize record to the appropriate JSONL file."""
        filename = _FILE_MAP.get(content_type, "unclassified.jsonl")
        line = orjson.dumps(record) + b"\n"

        with self._lock:
            if filename not in self._handles:
                path = self._dir / filename
                self._handles[filename] = open(path, "ab")
                self._counts[filename] = 0
                logger.info(f"Opened JSONL output: {path}")

            self._handles[filename].write(line)
            self._counts[filename] = self._counts.get(filename, 0) + 1

    def write_many(self, content_type: ContentType, records: list[dict]) -> None:
        """Write multiple records of the same type."""
        for record in records:
            self.write(content_type, record)

    def close(self) -> None:
        """Flush and close all open file handles."""
        with self._lock:
            for fh in self._handles.values():
                fh.flush()
                fh.close()
            self._handles.clear()

    def summary(self) -> dict[str, int]:
        """Return record counts per file."""
        return dict(self._counts)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
