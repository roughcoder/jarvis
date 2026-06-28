"""Flat markdown front-matter parsing shared by file-backed domains."""

from __future__ import annotations

import re

_FRONT_MATTER = re.compile(r"^\s*---\s*\n(.*?)\n---\s*(?:\n|$)", re.DOTALL)
_INLINE_LIST = re.compile(r"^\[(.*)\]$")


def parse_front_matter(text: str) -> dict[str, object]:
    """Parse the flat front-matter schema used by Jarvis markdown files."""
    m = _FRONT_MATTER.match(text)
    if not m:
        return {}
    out: dict[str, object] = {}
    lines = m.group(1).splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        i += 1
        if not line.strip() or line.lstrip().startswith("#") or ":" not in line:
            continue
        if line[0].isspace():
            continue
        key, _, rest = line.partition(":")
        key = key.strip()
        rest = rest.strip()
        if not rest:
            items: list[str] = []
            while i < len(lines) and (lines[i].lstrip().startswith("-") or not lines[i].strip()):
                item = lines[i].lstrip()
                i += 1
                if item.startswith("-"):
                    items.append(item[1:].strip().strip("'\""))
            out[key] = items
        elif _INLINE_LIST.match(rest):
            inner = _INLINE_LIST.match(rest).group(1)
            out[key] = [x.strip().strip("'\"") for x in inner.split(",") if x.strip()]
        else:
            out[key] = rest.strip("'\"")
    return out
