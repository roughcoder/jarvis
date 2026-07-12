"""Shared atomic-JSON-write helper for on-disk stores.

Several modules persist small JSON documents (the device registry, the
memory outbox, the conclusion-metadata sidecar, the upload manifest, the
orchestration session-ref/archive stores, the cockpit thread store, the MCP
status snapshot) and each hand-rolled its own write-then-replace-then-fsync
sequence. This is the one copy: write to a sibling tempfile in the same
directory, fsync the file, `os.replace` it into place, then best-effort
fsync the containing directory so the rename itself is durable.

Byte-level contract: the document is always terminated with a single
trailing newline. Callers that previously wrote without the newline or the
fsync inherited both on consolidation — a deliberate hardening, relevant
only to anything diffing exact bytes.

This is a dependency-free leaf (stdlib only, mirroring `jarvis/redaction.py`)
so both the brain tier and orchestration/connectors tier can import it
without crossing the brain/orchestration facade boundary.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


def atomic_write_json(
    path: Path,
    data: Any,
    *,
    indent: int | None = 2,
    sort_keys: bool = True,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        tmp = Path(handle.name)
        json.dump(data, handle, indent=indent, sort_keys=sort_keys)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    try:
        dir_fd = os.open(path.parent, os.O_DIRECTORY)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
