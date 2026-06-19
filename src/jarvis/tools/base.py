"""Tool primitives — the registry and the gated executor (Phase 3 §6/§7).

A `Tool` is a single capability-bearing action with a JSON-Schema signature the
model can call. The registry offers the model ONLY tools whose required
capability the request's context grants (deny-by-default), and `execute()`
re-checks the gate as defense in depth and applies a hard per-call timeout (the
hot path must never hang on a tool).
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from jarvis.brain.capabilities import require
from jarvis.brain.context import RequestContext

Handler = Callable[[RequestContext, dict[str, Any]], "Awaitable[str] | str"]


class ToolError(RuntimeError):
    """A tool failed — surfaced back to the model as a result, not fatal."""


@dataclass(frozen=True)
class Tool:
    name: str
    description: str
    parameters: dict[str, Any]  # JSON Schema for the function arguments
    required_capability: str
    handler: Handler
    # True for slow/remote tools (web search) that warrant a "looking that up"
    # earcon. Instant local tools (files, time) leave it False — no beep.
    announce: bool = False
    # Extra capabilities that must ALSO all be granted to offer/run this tool. Used
    # by skills (§7): a skill composes tools, so it's only offered when the context
    # grants every tool it would use — it can never exceed its profile's powers.
    extra_capabilities: frozenset[str] = frozenset()
    # True for tools whose result is a base64 PNG/JPEG image rather than text — the
    # tool loop feeds it to Jarvis's multimodal model as an image (native vision).
    produces_image: bool = False
    # Per-call timeout override (seconds). None => the registry's default hot-path
    # guard. Set for inherently slow tools (control_mac drives the screen for up to a
    # couple of minutes) so they aren't cancelled mid-run with an empty timeout error.
    timeout_s: float | None = None

    def openai_schema(self) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        self._tools[tool.name] = tool

    def get(self, name: str) -> Tool | None:
        return self._tools.get(name)

    def available_for(self, ctx: RequestContext) -> list[Tool]:
        """Tools whose required capability — and every extra capability — this
        context grants (deny-by-default)."""
        return [
            t
            for t in self._tools.values()
            if ctx.can(t.required_capability) and t.extra_capabilities <= ctx.capabilities
        ]

    async def execute(
        self,
        ctx: RequestContext,
        name: str,
        args: dict[str, Any],
        *,
        timeout_s: float,
    ) -> str:
        """Run a tool after re-checking the gate. Raises ToolError/CapabilityError
        on failure (the caller feeds the message back to the model)."""
        tool = self.get(name)
        if tool is None:
            raise ToolError(f"unknown tool {name!r}")
        require(ctx, tool.required_capability)  # also filtered at offer time
        for cap in tool.extra_capabilities:  # skills: every composed tool's cap
            require(ctx, cap)
        result = tool.handler(ctx, args)
        if inspect.isawaitable(result):
            # A slow tool (control_mac) may set its own budget; else the hot-path guard.
            eff = tool.timeout_s or timeout_s
            try:
                result = await asyncio.wait_for(result, eff)
            except TimeoutError as exc:  # bare TimeoutError stringifies to '' — be legible
                raise ToolError(f"{name} timed out after {eff:.0f}s") from exc
        return str(result)
