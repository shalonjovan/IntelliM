"""
cleaner/rating_cleaner.py — Normalizes rating values to a 0–5 float scale.

Handles:
  - "4.2 out of 5 stars" → 4.2
  - "4.2/5" → 4.2
  - "8.4/10" → 4.2 (rescaled)
  - "4.2" → 4.2
  - "84%" → 4.2 (rescaled from percentage)
  - "(1,234 ratings)" → count=1234
"""

from __future__ import annotations

import re

_RATING_RE = re.compile(
    r"(?P<value>[\d.]+)\s*(?:out\s+of\s+(?P<scale>[\d.]+)|/(?P<denom>[\d.]+))?",
    re.I,
)
_PERCENT_RE = re.compile(r"(?P<pct>[\d.]+)\s*%")
_COUNT_RE = re.compile(r"([\d,]+)\s*(?:ratings?|reviews?|global)", re.I)


class RatingCleaner:
    """Stateless rating cleaner."""

    def clean(self, raw: str) -> dict[str, float | int | str | None]:
        """
        Parse a rating string into a structured dict.

        Returns:
            {"value": float | None, "raw": str}  — value is on 0–5 scale
        """
        if not raw:
            return {"value": None, "raw": raw}

        raw = raw.strip()

        # Percentage format
        pct_match = _PERCENT_RE.search(raw)
        if pct_match:
            pct = float(pct_match.group("pct"))
            return {"value": round(pct / 20, 2), "raw": raw}

        # Numeric format (with optional /N or "out of N")
        match = _RATING_RE.search(raw)
        if not match:
            return {"value": None, "raw": raw}

        value = float(match.group("value"))
        scale = match.group("scale") or match.group("denom")
        if scale:
            scale_f = float(scale)
            # Normalize to 5-point scale
            if scale_f != 5:
                value = round((value / scale_f) * 5, 2)
        else:
            # Assume 5-point if value ≤ 5, 10-point if value ≤ 10 without denom
            if value > 5:
                value = round((value / 10) * 5, 2)

        value = min(5.0, max(0.0, value))
        return {"value": round(value, 2), "raw": raw}

    def clean_count(self, raw: str) -> int | None:
        """Extract a review/rating count integer."""
        match = _COUNT_RE.search(raw)
        if match:
            try:
                return int(match.group(1).replace(",", ""))
            except ValueError:
                pass
        return None


# module-level singleton
rating_cleaner = RatingCleaner()
