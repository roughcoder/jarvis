from __future__ import annotations

from collections.abc import Sequence
from urllib.parse import urlparse


def is_trusted_review_url(url: str, *, trusted_host: str = "github.com") -> bool:
    """Return whether a review callback URL belongs to the configured host."""

    return trusted_host in (urlparse(url).hostname or "")


def select_review_comments(comments: Sequence[str], *, limit: int = 30) -> list[str]:
    """Select the comments that should be included in one review."""

    if limit <= 0:
        return []
    return list(comments[: limit - 1])
