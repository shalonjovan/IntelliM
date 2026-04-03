"""
queue/request_models.py — typed helpers for building and unpacking
Crawlee Request objects that carry CrawlMeta in user_data.
"""

from __future__ import annotations

from crawlee import Request

from classifier.page_types import PageType
from models import CrawlMeta


def make_request(
    url: str,
    meta: CrawlMeta,
    page_type: PageType = PageType.UNKNOWN,
    label: str | None = None,
) -> Request:
    """
    Build a Crawlee Request with CrawlMeta embedded in user_data.

    The label is used by Crawlee's router to dispatch to the right handler.
    If not supplied it falls back to the page_type queue name.
    """
    user_data = meta.to_dict()
    user_data["page_type"] = page_type.value

    return Request.from_url(
        url=url,
        user_data=user_data,
        label=label or page_type.queue_name,
    )


def extract_meta(request: Request) -> CrawlMeta:
    """Pull the CrawlMeta out of a Crawlee Request's user_data."""
    return CrawlMeta.from_dict(request.user_data)


def extract_page_type(request: Request) -> PageType:
    """Pull the PageType out of a Crawlee Request's user_data."""
    raw = request.user_data.get("page_type", PageType.UNKNOWN.value)
    try:
        return PageType(raw)
    except ValueError:
        return PageType.UNKNOWN