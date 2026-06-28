"""Heartbeat — proactive, cold-path reach-out (Phase 3b, §9).

A scheduler that periodically works the `HEARTBEAT.md` checklist and pushes a
message to connected intercoms ONLY when there's something genuinely worth saying.
Two invariants from the spec:

- **Silent-completion sentinel** (their `NO_REPLY`): a heartbeat that decides
  nothing's worth saying produces *no* output and never streams a partial —
  essential for a speaking assistant.
- **Transcript hygiene**: heartbeat output never enters the conversational
  transcript that feeds the voice prompt — it's a separate, server-initiated push.

It runs entirely off the hot path (a background task), so it can never delay a
voice turn. The LLM/checklist work is injected as `think` so the scheduler itself
is pure and unit-testable.
"""

from __future__ import annotations

import asyncio
import pathlib
import time
from collections.abc import Awaitable, Callable

from jarvis.config import Config, HeartbeatConfig

_HEARTBEAT_PROMPT = (
    "You are Jarvis running a quiet background check — the user did NOT ask "
    "anything. Work through the checklist below. If there is something genuinely "
    "worth telling them right now, say it in one or two natural spoken sentences. "
    "If there is nothing worth interrupting them for, reply with exactly {sentinel} "
    "and nothing else. When in doubt, stay silent ({sentinel})."
)


def is_silent(text: str, sentinel: str) -> bool:
    """True when the heartbeat produced nothing worth saying (empty or the sentinel)."""
    t = (text or "").strip()
    return (not t) or (sentinel.upper() in t.upper())


def make_heartbeat_think(cfg: Config) -> Callable[[], Awaitable[str]]:
    """The default `think`: read HEARTBEAT.md and ask the model whether anything is
    worth saying (returns the sentinel when not). Built around the gateway client."""
    from jarvis.brain.gateway_client import GatewayClient, LLMAttribution

    gateway = GatewayClient(cfg.gateway)
    heartbeat_gateway = gateway.with_attribution(
        LLMAttribution(kind="heartbeat", channel="system", speaker="heartbeat")
    )

    async def think() -> str:
        path = pathlib.Path(cfg.heartbeat.path)
        checklist = path.read_text(encoding="utf-8") if path.exists() else ""
        if not checklist.strip():
            return cfg.heartbeat.sentinel
        messages = [
            {"role": "system", "content": _HEARTBEAT_PROMPT.format(sentinel=cfg.heartbeat.sentinel)},
            {"role": "user", "content": checklist},
        ]
        return await heartbeat_gateway.complete(messages, model=cfg.gateway.fast_model)

    return think


class HeartbeatScheduler:
    def __init__(
        self,
        cfg: HeartbeatConfig,
        *,
        think: Callable[[], Awaitable[str]],
        broadcast: Callable[[str], Awaitable[None]],
        tracer=None,  # noqa: ANN001 - optional Tracer; heartbeat stays pure in tests
        room: str = "",
    ) -> None:
        self._cfg = cfg
        self._think = think
        self._broadcast = broadcast
        self._tracer = tracer
        self._room = room

    async def tick(self) -> str | None:
        """Run one check; broadcast + return the message if meaningful, else None."""
        t0 = time.perf_counter()
        text = await self._think()
        silent = is_silent(text, self._cfg.sentinel)
        if self._tracer is not None:
            trace = self._tracer.turn(
                room=self._room,
                speaker="heartbeat",
                channel="system",
                kind="heartbeat",
            )
            trace.stage(
                "llm",
                (time.perf_counter() - t0) * 1000,
                emitted=not silent,
                chars=len((text or "").strip()),
            )
            self._tracer.emit(trace)
        if silent:
            return None
        await self._broadcast(text.strip())
        return text.strip()

    async def run(self) -> None:
        """Loop forever on the cold path. Each tick is guarded — a heartbeat failure
        must never crash the brain."""
        while True:
            await asyncio.sleep(self._cfg.interval_s)
            try:
                await self.tick()
            except Exception as exc:  # noqa: BLE001 - proactive work is best-effort
                print(f"  [heartbeat] skipped: {exc}")
