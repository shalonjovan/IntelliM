"""
cleaner/price_cleaner.py — Extracts and normalizes price information.

Handles:
  - Indian Rupee formats: ₹1,49,999 / Rs.1,49,999 / INR 14999
  - USD/EUR/GBP formats: $1,299.99
  - Numeric-only strings from JSON-LD: "149999"
  - Returns {"amount": float, "currency": str}
"""

from __future__ import annotations

import re

_CURRENCY_SYMBOLS: dict[str, str] = {
    "₹": "INR",
    "rs": "INR",
    "inr": "INR",
    "$": "USD",
    "usd": "USD",
    "€": "EUR",
    "eur": "EUR",
    "£": "GBP",
    "gbp": "GBP",
}

# Matches optional currency prefix + number with optional commas/decimals
_PRICE_RE = re.compile(
    r"(?P<sym>[₹$€£]|rs\.?|inr|usd|eur|gbp)?\s*"
    r"(?P<num>[\d,]+(?:\.\d{1,2})?)",
    re.I,
)


class PriceCleaner:
    """Stateless price cleaner."""

    def clean(self, raw: str) -> dict[str, float | str | None]:
        """
        Parse a price string into a structured dict.

        Returns:
            {"amount": float | None, "currency": str, "raw": str}
        """
        if not raw:
            return {"amount": None, "currency": "INR", "raw": raw}

        raw_stripped = raw.strip()
        match = _PRICE_RE.search(raw_stripped)
        if not match:
            return {"amount": None, "currency": "INR", "raw": raw_stripped}

        sym_raw = (match.group("sym") or "").strip().lower()
        currency = _CURRENCY_SYMBOLS.get(sym_raw, "INR")

        num_str = match.group("num").replace(",", "")
        try:
            amount = float(num_str)
        except ValueError:
            amount = None

        return {"amount": amount, "currency": currency, "raw": raw_stripped}


# module-level singleton
price_cleaner = PriceCleaner()
