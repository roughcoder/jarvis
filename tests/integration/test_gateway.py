"""Integration: real LLM gateway round-trip (formalises `jarvis ping-gateway`).

Proves the fast + strong routes return completions and that streaming yields
deltas — i.e. "it talks to the LLM". Skips cleanly if the gateway is down.
"""

from __future__ import annotations

import asyncio
import socket

import pytest

from jarvis.config import load_config
from jarvis.brain.gateway_client import GatewayClient

pytestmark = pytest.mark.integration


def _cfg_or_skip():
    cfg = load_config()
    try:
        with socket.create_connection((cfg.gateway.host, cfg.gateway.port), timeout=1.0):
            pass
    except OSError:
        pytest.skip(f"gateway not reachable at {cfg.gateway.base_url}")
    return cfg


def _complete(cfg, model: str) -> str:
    async def run() -> str:
        c = GatewayClient(cfg.gateway)
        try:
            return await c.complete(
                [{"role": "user", "content": "Reply with exactly one short sentence."}],
                model=model,
            )
        finally:
            await c.aclose()

    return asyncio.run(run())


def test_fast_route_completes() -> None:
    cfg = _cfg_or_skip()
    out = _complete(cfg, cfg.gateway.fast_model)
    assert isinstance(out, str) and out.strip()


def test_strong_route_completes() -> None:
    cfg = _cfg_or_skip()
    out = _complete(cfg, cfg.gateway.strong_model)
    assert isinstance(out, str) and out.strip()


def test_stream_yields_tokens() -> None:
    cfg = _cfg_or_skip()

    async def run() -> str:
        c = GatewayClient(cfg.gateway)
        chunks: list[str] = []
        try:
            async for delta in c.stream(
                [{"role": "user", "content": "Count to three."}],
                model=cfg.gateway.fast_model,
            ):
                chunks.append(delta)
        finally:
            await c.aclose()
        return "".join(chunks)

    assert asyncio.run(run()).strip()
