"""Boundary encoders for Honcho v3 ids and local cache filenames."""

from __future__ import annotations

import base64
import re


_HONCHO_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
_PREFIX = "jv_"


def encode_honcho_id(jarvis_id: str) -> str:
    """Encode any Jarvis id into Honcho v3's restricted id alphabet.

    Honcho v3 rejects colons and other separators in resource ids. Jarvis keeps
    semantic ids such as `voice:neil:mac` and `project:jarvis` above the backend
    interface, so the v3 client encodes at the boundary using URL-safe base64.
    Honcho-safe ids such as `jarvis-dev` and `neil` pass through unchanged. The
    `jv_` prefix marks encoded ids and avoids collisions when a Jarvis id itself
    starts with that prefix.
    """
    if _HONCHO_ID_RE.match(jarvis_id) and not jarvis_id.startswith(_PREFIX):
        return jarvis_id
    raw = jarvis_id.encode("utf-8")
    token = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    return f"{_PREFIX}{token}"


def decode_honcho_id(honcho_id: str) -> str:
    if not honcho_id.startswith(_PREFIX):
        return honcho_id
    token = honcho_id[len(_PREFIX) :]
    padded = token + ("=" * (-len(token) % 4))
    try:
        return base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        # Prefixed but not ours (we only decode ids we encoded); pass through.
        return honcho_id


def assert_honcho_safe(honcho_id: str) -> str:
    if not _HONCHO_ID_RE.match(honcho_id):
        raise ValueError(f"encoded Honcho id is not v3-safe: {honcho_id!r}")
    return honcho_id


def cache_key(peer_id: str) -> str:
    """Injective, readable filesystem-safe peer key for representation caches.

    Cache filenames are local operator artifacts, not Honcho boundary ids. Keep
    simple peer ids byte-compatible while mapping semantic separators such as
    `project:jarvis` to `project-jarvis` instead of base64. Literal hyphens and
    other non-alphanumeric characters are escaped as UTF-8 byte tokens, so ids
    that differ only by separators cannot collapse onto the same cache file.
    """
    parts: list[str] = []
    for char in peer_id:
        if char.isascii() and char.isalnum():
            parts.append(char)
        elif char == ":":
            parts.append("-")
        else:
            parts.extend(f"_x{byte:02x}_" for byte in char.encode("utf-8"))
    return "".join(parts) or "peer"
