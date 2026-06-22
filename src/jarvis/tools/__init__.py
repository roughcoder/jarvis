"""Tools: capability-gated actions the brain can invoke (Phase 3 §6).

`build_registry()` registers every tool; the registry then offers the model only
those a request's context grants (deny-by-default), so registration is not a
grant — the capability gate is.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from jarvis.config import (
    BrowserConfig,
    CapabilityConfig,
    GoogleConfig,
    RemoteConfig,
    ToolsConfig,
    WorkerConfig,
)
from jarvis.tools.base import Tool, ToolError, ToolRegistry
from jarvis.tools.browser import make_browser_tools
from jarvis.tools.files import make_files_tools
from jarvis.tools.google import make_google_tools
from jarvis.tools.fetch import make_fetch_tools
from jarvis.tools.profile import make_profile_tools
from jarvis.tools.remote import make_remote_tools
from jarvis.tools.web_search import make_web_search_tool
from jarvis.tools.worker import make_worker_tools

if TYPE_CHECKING:
    from jarvis.brain.memory_client import MemoryClient

__all__ = ["Tool", "ToolError", "ToolRegistry", "build_registry"]


def build_registry(
    cfg: ToolsConfig,
    *,
    worker: WorkerConfig | None = None,
    remote: RemoteConfig | None = None,
    google: GoogleConfig | None = None,
    browser: BrowserConfig | None = None,
    capabilities: CapabilityConfig | None = None,
    memory: MemoryClient | None = None,
    mcp: list[Tool] | None = None,
) -> ToolRegistry:
    """Register every tool. `worker` adds the local worker-daemon tools, `remote`
    the cloud (Managed Agents) tools, `mcp` the bridged MCP-server tools (built by
    `make_mcp_tools` after the bridge has connected) — all thin clients,
    capability-gated like the rest, so registering them is not a grant. Pass None
    to omit. MCP tools come pre-built because discovery is async (off the hot
    path); the brain connects the bridge at startup, then registers the result."""
    reg = ToolRegistry()
    reg.register(make_web_search_tool(cfg))
    for tool in make_files_tools(cfg):
        reg.register(tool)
    for tool in make_fetch_tools(cfg):  # generic: fetch a URL as clean text (for skills)
        reg.register(tool)
    if capabilities is not None:
        for tool in make_profile_tools(capabilities, memory=memory):
            reg.register(tool)
    if worker is not None:
        for tool in make_worker_tools(worker):
            reg.register(tool)
        # Browser lane shares the worker's HTTP boundary (the host lives in the worker).
        if browser is not None and browser.enabled:
            for tool in make_browser_tools(worker, browser):
                reg.register(tool)
    if remote is not None:
        for tool in make_remote_tools(remote):
            reg.register(tool)
    if google is not None:
        for tool in make_google_tools(google):
            reg.register(tool)
    for tool in mcp or []:
        reg.register(tool)
    return reg
