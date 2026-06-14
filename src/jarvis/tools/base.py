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
        """Tools whose required capability this context grants (deny-by-default)."""
        return [t for t in self._tools.values() if ctx.can(t.required_capability)]

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
        result = tool.handler(ctx, args)
        if inspect.isawaitable(result):
            result = await asyncio.wait_for(result, timeout_s)
        return str(result)
