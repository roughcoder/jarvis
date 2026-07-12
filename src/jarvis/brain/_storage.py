"""Compatibility shim: the canonical home for atomic_write_json is
jarvis.storage (a dependency-free leaf, mirroring jarvis/redaction.py). This
module re-exports it so existing brain/ callers are untouched.
"""

from __future__ import annotations

from jarvis.storage import atomic_write_json

__all__ = ["atomic_write_json"]
