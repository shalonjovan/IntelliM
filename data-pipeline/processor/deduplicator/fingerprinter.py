"""
deduplicator/fingerprinter.py — Content fingerprinting and URL normalization.

Generates a deterministic hash for a product record to identify duplicates
across different source domains and URL variants.

Fingerprint strategy:
  1. URL normalization — strip tracking params, canonicalize scheme
  2. Field hash — sha256(brand + model_number + price_amount + domain)
     This catches same product listed at different URL paths (e.g. Amazon vs Flipkart)
"""

from __future__ import annotations

import hashlib
import re
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse


# Query params that are tracking params (strip before comparing URLs)
_STRIP_PARAMS = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "ref", "ref_", "pf_rd_p", "pf_rd_r", "pf_rd_s", "pf_rd_t",
    "smid", "tag", "linkCode", "linkId", "pd_rd_wg", "pd_rd_w",
    "gclid", "fbclid", "msclkid", "_ga", "aff_id",
    "clickid", "affid", "affiliate", "source",
})


def normalize_url(url: str) -> str:
    """
    Strip tracking query parameters and normalize the URL.

    Returns a canonical URL string suitable for deduplication lookup.
    """
    if not url:
        return ""
    try:
        parsed = urlparse(url.strip().lower())
        qs = parse_qs(parsed.query, keep_blank_values=False)
        clean_qs = {k: v for k, v in qs.items() if k not in _STRIP_PARAMS}
        canonical = urlunparse((
            parsed.scheme or "https",
            parsed.netloc,
            parsed.path.rstrip("/"),
            "",
            urlencode(clean_qs, doseq=True),
            "",   # strip fragment
        ))
        return canonical
    except Exception:
        return url


def field_fingerprint(
    brand: str,
    product_name: str,
    model_number: str = "",
    price_amount: float | None = None,
    seed_category: str = "",
) -> str:
    """
    Generate a content hash for deduplication across different source URLs.

    Two records with the same brand + cleaned model name are treated as the same product
    regardless of which domain or URL they came from.

    Returns:
        8-char hex prefix of sha256 (good enough for collision avoidance at this scale)
    """
    brand_clean    = _clean_token(brand)
    model_clean    = _clean_token(model_number or product_name)
    category_clean = _clean_token(seed_category)

    # Include price only for disambiguation within same brand+model (different variants)
    price_str = f"{price_amount:.0f}" if price_amount else ""

    key = "|".join([brand_clean, model_clean, category_clean, price_str])
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return digest[:16]


def url_fingerprint(url: str) -> str:
    """Hash of the normalized URL — for exact URL-level dedup."""
    normalized = normalize_url(url)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


# ------------------------------------------------------------------ helpers

_NON_ALNUM = re.compile(r"[^a-z0-9]+")

def _clean_token(text: str) -> str:
    """Lowercase, remove non-alphanumeric, collapse spaces."""
    return _NON_ALNUM.sub("", text.lower().strip())
