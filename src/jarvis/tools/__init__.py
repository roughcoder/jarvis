"""Tools: capability-gated actions the brain can invoke (Phase 3 §6).

`build_registry()` registers every tool; the registry then offers the model only
those a request's context grants (deny-by-default), so registration is not a
grant — the capability gate is.
"""

from __future__ import annotations

from jarvis.config import RemoteConfig, ToolsConfig, WorkerConfig
from jarvis.tools.base import Tool, ToolError, ToolRegistry
from jarvis.tools.files import make_files_tools
from jarvis.tools.remote import make_remote_tools
from jarvis.tools.web_search import make_web_search_tool
from jarvis.tools.worker import make_worker_tools

__all__ = ["Tool", "ToolError", "ToolRegistry", "build_registry"]


def build_registry(
    cfg: ToolsConfig,
    *,
    worker: WorkerConfig | None = None,
    remote: RemoteConfig | None = None,
) -> ToolRegistry:
    """Register every tool. `worker` adds the local worker-daemon tools, `remote`
    the cloud (Managed Agents) tools — both thin HTTP clients, capability-gated
    like the rest, so registering them is not a grant. Pass None to omit."""
    reg = ToolRegistry()
    reg.register(make_web_search_tool(cfg))
    for tool in make_files_tools(cfg):
        reg.register(tool)
    if worker is not None:
        for tool in make_worker_tools(worker):
            reg.register(tool)
    if remote is not None:
        for tool in make_remote_tools(remote):
            reg.register(tool)
    return reg
