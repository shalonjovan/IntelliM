"""
cleaner/text_cleaner.py — Normalizes raw text strings.

Handles:
  - HTML entity decoding
  - Whitespace normalization
  - Unicode control character stripping
  - Truncation to a safe max length
"""

from __future__ import annotations

import html
import re
import unicodedata


_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_WHITESPACE_RE = re.compile(r"\s+")


class TextCleaner:
    """
    Stateless text cleaner. All methods are pure functions.
    """

    MAX_FIELD_LEN: int = 5000

    def clean(self, text: str, max_len: int | None = None) -> str:
        """
        Full pipeline: decode entities → strip control chars → normalize whitespace → truncate.
        """
        if not text:
            return ""
        text = html.unescape(text)
        text = self._strip_control(text)
        text = _WHITESPACE_RE.sub(" ", text).strip()
        text = unicodedata.normalize("NFKC", text)
        limit = max_len if max_len is not None else self.MAX_FIELD_LEN
        return text[:limit]

    def clean_short(self, text: str) -> str:
        """Clean and limit to 500 chars (suitable for names, titles)."""
        return self.clean(text, max_len=500)

    def clean_long(self, text: str) -> str:
        """Clean and allow up to 5000 chars (descriptions, review bodies)."""
        return self.clean(text, max_len=5000)

    def clean_list(self, items: list[str]) -> list[str]:
        """Clean a list of text items, dropping empty results."""
        return [c for item in items if (c := self.clean_short(item))]

    # ------------------------------------------------------------------ private

    @staticmethod
    def _strip_control(text: str) -> str:
        return _CONTROL_RE.sub("", text)


# module-level singleton
text_cleaner = TextCleaner()
