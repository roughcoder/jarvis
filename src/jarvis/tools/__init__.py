"""Tools: capability-gated actions the brain can invoke (Phase 3 §6).

`build_registry()` registers every tool; the registry then offers the model only
those a request's context grants (deny-by-default), so registration is not a
grant — the capability gate is.
"""

from __future__ import annotations

from jarvis.config import ToolsConfig
from jarvis.tools.base import Tool, ToolError, ToolRegistry
from jarvis.tools.files import make_files_tools
from jarvis.tools.web_search import make_web_search_tool

__all__ = ["Tool", "ToolError", "ToolRegistry", "build_registry"]


def build_registry(cfg: ToolsConfig) -> ToolRegistry:
    reg = ToolRegistry()
    reg.register(make_web_search_tool(cfg))
    for tool in make_files_tools(cfg):
        reg.register(tool)
    return reg
