"""files tools — root-bounded read/list/write (fs-safe pattern, Phase 3 §6).

All paths resolve *within* a configured root; `..`, absolute paths, and symlink
escapes are rejected. Read and list need `files.read`; write needs the more
privileged `files.write` — so a device granted only read never sees `write_file`.
"""

from __future__ import annotations

import pathlib
from typing import Any

from jarvis.runtime import RequestContext
from jarvis.config import ToolsConfig
from jarvis.tools.base import Tool, ToolError

READ_CAP = "files.read"
WRITE_CAP = "files.write"


def _resolve_within(root: str, requested: str) -> pathlib.Path:
    """Resolve `requested` under `root`, rejecting any escape (.. / abs / symlink)."""
    base = pathlib.Path(root).resolve()
    req = pathlib.Path(requested or ".")
    if req.is_absolute():
        raise ToolError("absolute paths are not allowed")
    target = (base / req).resolve()
    if target != base and base not in target.parents:
        raise ToolError("path escapes the files root")
    return target


def make_files_tools(cfg: ToolsConfig) -> list[Tool]:
    root = cfg.files_root

    def read_file(ctx: RequestContext, args: dict[str, Any]) -> str:
        p = _resolve_within(root, args.get("path", ""))
        if not p.is_file():
            return f"error: no such file: {args.get('path')!r}"
        return p.read_text(encoding="utf-8", errors="replace")

    def list_files(ctx: RequestContext, args: dict[str, Any]) -> str:
        p = _resolve_within(root, args.get("path", "."))
        if not p.exists():
            return "error: no such directory"
        if p.is_file():
            return p.name
        entries = sorted(c.name + ("/" if c.is_dir() else "") for c in p.iterdir())
        return "\n".join(entries) if entries else "(empty)"

    def write_file(ctx: RequestContext, args: dict[str, Any]) -> str:
        p = _resolve_within(root, args.get("path", ""))
        content = args.get("content", "")
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
        return f"wrote {len(content)} chars to {args.get('path')}"

    path_arg = {"path": {"type": "string", "description": "Path relative to the workspace root."}}
    return [
        Tool(
            name="read_file",
            description="Read a text file from the workspace.",
            parameters={"type": "object", "properties": path_arg, "required": ["path"]},
            required_capability=READ_CAP,
            handler=read_file,
        ),
        Tool(
            name="list_files",
            description="List the files in a workspace directory (default: root).",
            parameters={"type": "object", "properties": path_arg},
            required_capability=READ_CAP,
            handler=list_files,
        ),
        Tool(
            name="write_file",
            description="Create or overwrite a text file in the workspace.",
            parameters={
                "type": "object",
                "properties": {
                    **path_arg,
                    "content": {"type": "string", "description": "The text to write."},
                },
                "required": ["path", "content"],
            },
            required_capability=WRITE_CAP,
            handler=write_file,
        ),
    ]
