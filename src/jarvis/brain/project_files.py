"""@-mention resolution for project (memory) files.

A turn may name an ingested project file inline — `@spec.md` or
`@memory:spec-a1b2c3d4e5f6`. On turn receipt we look the handle up in the
project's upload manifest and append a bounded context block so the provider
sees the content without the caller pasting it.

Resolution is brain-side by construction: the manifest and `files_root` live on
the brain host, so the worker never needs to reach the vault — only the
rendered text crosses the HTTP boundary. Every failure mode here (missing file,
unreadable bytes, corrupt manifest) degrades to leaving the mention as typed;
per AGENTS.md a file read must never break a turn.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

from jarvis.brain.project_management import project_file_rows, stored_filename
from jarvis.config import Config

logger = logging.getLogger(__name__)

# `@memory:<doc_id>` is always a mention; bare `@<filename>` only when the token
# carries no whitespace (the pinned semantics — stored names are doc_id-derived
# and so never contain any). Both forms stop at whitespace.
_MENTION_RE = re.compile(r"(?<![\w@])@(memory:)?([^\s@]+)")
# Trailing sentence punctuation is almost never part of a filename ("read
# @spec.md, then...") but a dot is, so strip only what cannot end a stored name.
_TRAILING_PUNCTUATION = ",;:!?)]}'\"" + "’”"


def find_mentions(text: str) -> list[str]:
    """Return the mention handles in `text`, in order, de-duplicated.

    Handles keep their `memory:` prefix so the caller can tell the two forms
    apart; matching is done by `resolve_project_mentions`.
    """
    seen: list[str] = []
    for match in _MENTION_RE.finditer(text or ""):
        handle = (match.group(1) or "") + match.group(2).rstrip(_TRAILING_PUNCTUATION)
        if handle and handle not in seen and handle not in {"memory:"}:
            seen.append(handle)
    return seen


def _row_matches(row: dict[str, Any], handle: str) -> bool:
    doc_id = str(row.get("doc_id") or "")
    if handle.startswith("memory:"):
        return bool(doc_id) and doc_id == handle.removeprefix("memory:")
    return handle in {value for value in (doc_id, stored_filename(row)) if value}


def _render_block(row: dict[str, Any], handle: str, max_bytes: int) -> str:
    """Render one bounded context block for a matched manifest row."""
    label = stored_filename(row) or str(row.get("doc_id") or handle)
    header = f"--- @file {label} (project file) ---"
    mime_type = str(row.get("mime_type") or "")
    path = Path(str(row.get("original_path") or ""))
    try:
        data = path.read_bytes()
    except OSError:
        logger.warning("project file mention could not be read: %s", path, exc_info=True)
        return "\n".join([header, f"[unavailable: {label} ({mime_type or 'unknown type'}) could not be read]"])
    truncated = len(data) > max_bytes
    try:
        content = data[:max_bytes].decode("utf-8")
    except UnicodeDecodeError:
        # Binary (or a cut mid-codepoint): send metadata instead of bytes.
        return "\n".join(
            [
                header,
                f"[binary file: {label}, {mime_type or 'unknown type'}, {len(data)} bytes — content not inlined]",
            ]
        )
    lines = [header, content]
    if truncated:
        lines.append(f"[truncated at {max_bytes} bytes of {len(data)}]")
    return "\n".join(lines)


def resolve_project_mentions(cfg: Config, project_id: str, text: str) -> str:
    """Append a context block per resolvable `@file` mention in `text`.

    The mention itself is left in place so the model sees what the user typed.
    Unresolvable handles pass through untouched — an `@` in prose is not an
    error. Returns `text` unchanged when nothing resolves.
    """
    if not project_id or not text or "@" not in text:
        return text
    handles = find_mentions(text)
    if not handles:
        return text
    try:
        rows = project_file_rows(cfg, project_id)
    except Exception:  # noqa: BLE001 - a corrupt manifest must not break the turn.
        logger.warning("project file manifest unavailable for mentions: %s", project_id, exc_info=True)
        return text
    max_bytes = max(1, int(cfg.registry.mention_content_max_bytes))
    max_files = max(0, int(cfg.registry.mention_max_files))
    blocks: list[str] = []
    for handle in handles:
        if len(blocks) >= max_files:
            break
        row = next((candidate for candidate in rows if _row_matches(candidate, handle)), None)
        if row is None:
            continue
        try:
            blocks.append(_render_block(row, handle, max_bytes))
        except Exception:  # noqa: BLE001 - best-effort; drop this block, keep the turn.
            logger.warning("project file mention could not be rendered: %s", handle, exc_info=True)
    if not blocks:
        return text
    return "\n\n".join([text, *blocks])
