"""Runtime contracts shared by brain orchestration and tool adapters.

This module deliberately imports no brain, tool, or infrastructure packages. It
keeps the capability envelope and tool registry on a neutral boundary so brain
composition can depend on tools without tool adapters depending back on brain
internals.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RequestContext:
    device_id: str
    identity: str
    scope: str
    capabilities: frozenset[str]
    channel: str = "voice"
    confidence: str = "strong"
    peer: str = ""

    def can(self, capability: str) -> bool:
        return capability in self.capabilities

    @property
    def memory_peer(self) -> str:
        """Honcho peer this request's memory is scoped to."""
        return self.peer or self.identity


class CapabilityError(PermissionError):
    """Raised when a request lacks a required capability."""


def require(ctx: RequestContext, capability: str) -> None:
    """Gate a capability-bearing action. Raises CapabilityError if not granted."""
    if not ctx.can(capability):
        raise CapabilityError(
            f"capability {capability!r} not granted "
            f"(identity={ctx.identity!r}, device={ctx.device_id!r})"
        )


Handler = Callable[[RequestContext, dict[str, Any]], "Awaitable[str] | str"]


class ToolError(RuntimeError):
    """A tool failed; callers surface this to the model as a result."""


@dataclass(frozen=True)
class Tool:
    name: str
    description: str
    parameters: dict[str, Any]
    required_capability: str
    handler: Handler
    announce: bool = False
    extra_capabilities: frozenset[str] = frozenset()
    produces_image: bool = False
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
        """Tools whose required and extra capabilities this context grants."""
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
        """Run a tool after re-checking the capability gate."""
        tool = self.get(name)
        if tool is None:
            raise ToolError(f"unknown tool {name!r}")
        require(ctx, tool.required_capability)
        for cap in tool.extra_capabilities:
            require(ctx, cap)
        result = tool.handler(ctx, args)
        if inspect.isawaitable(result):
            eff = tool.timeout_s or timeout_s
            try:
                result = await asyncio.wait_for(result, eff)
            except TimeoutError as exc:
                raise ToolError(f"{name} timed out after {eff:.0f}s") from exc
        return str(result)
