"""
classifier/page_types.py — enum of all page types the classifier can assign.
"""

from enum import Enum


class PageType(str, Enum):
    PRODUCT_PAGE  = "product_page"    # single product detail page
    CATEGORY_PAGE = "category_page"   # listing / search results page
    REVIEW_PAGE   = "review_page"     # dedicated reviews / ratings page
    AD_CREATIVE   = "ad_creative"     # ad transparency / sponsored content page
    TREND_DATA    = "trend_data"      # search trend or popularity page
    UNKNOWN       = "unknown"         # classifier could not determine type

    @property
    def queue_name(self) -> str:
        """Maps a page type to its named Crawlee sub-queue."""
        return f"queue_{self.value}"