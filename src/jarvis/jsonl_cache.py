from __future__ import annotations

import os
import json
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar


T = TypeVar("T")
JsonlFingerprint = tuple[int, int, int, int]
JsonlCacheEntry = tuple[int, int, int, int, int, list[T]]


def jsonl_fingerprint(stat: os.stat_result) -> JsonlFingerprint:
    """Return the metadata used to detect append, truncate, and rotation."""

    return (stat.st_size, stat.st_mtime_ns, stat.st_dev, stat.st_ino)


def read_jsonl_projection(
    path: Path,
    stat: os.stat_result,
    cached: JsonlCacheEntry[T] | None,
    *,
    clone: Callable[[T], T],
    merge: Callable[[list[T], object], None],
) -> tuple[JsonlFingerprint, int, list[T]]:
    """Read complete JSONL records, tailing a compatible cached projection.

    The returned offset always stops before a partial final record. Callers can
    therefore reuse it after an append without ever caching incomplete JSON.
    """

    fingerprint = jsonl_fingerprint(stat)
    if cached is not None and cached[:4] == fingerprint:
        return fingerprint, cached[4], [clone(item) for item in cached[5]]

    if (
        cached is not None
        and cached[2:4] == fingerprint[2:]
        and stat.st_size > cached[0]
        and stat.st_size >= cached[4]
    ):
        items = [clone(item) for item in cached[5]]
        offset = cached[4]
    else:
        items = []
        offset = 0

    with path.open("rb") as handle:
        handle.seek(offset)
        for raw_line in handle.read().splitlines(keepends=True):
            if not raw_line.endswith((b"\n", b"\r")):
                break
            offset += len(raw_line)
            line = raw_line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            merge(items, record)

    return fingerprint, offset, items
