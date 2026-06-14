"""Gateway client — talks ONLY to the LiteLLM proxy over HTTP (spec §3.1, §4).

The turn loop never imports a provider SDK; it calls this module, which calls
the OpenAI-compatible LiteLLM endpoint. Model choice is a parameter (a LiteLLM
route name), so switching fast<->strong is config, not code (spec Step 1).
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from openai import AsyncOpenAI

from jarvis.config import GatewayConfig


class GatewayClient:
    def __init__(self, cfg: GatewayConfig) -> None:
        self._cfg = cfg
        # base_url points at the LiteLLM proxy; /v1 is the OpenAI-compatible path.
        self._client = AsyncOpenAI(
            base_url=f"{cfg.base_url}/v1",
            api_key=cfg.api_key.get_secret_value(),
            timeout=cfg.request_timeout_s,
        )

    def _resolve(self, model: str | None) -> str:
        # Default to the fast route; callers pass cfg.strong_model when needed.
        return model or self._cfg.fast_model

    async def complete(self, messages: list[dict], *, model: str | None = None) -> str:
        """Non-streaming completion. `model` is a LiteLLM route name."""
        resp = await self._client.chat.completions.create(
            model=self._resolve(model),
            messages=messages,  # type: ignore[arg-type]
        )
        return resp.choices[0].message.content or ""

    async def stream(
        self, messages: list[dict], *, model: str | None = None
    ) -> AsyncIterator[str]:
        """Streaming completion: yields text deltas for time-to-first-token."""
        stream = await self._client.chat.completions.create(
            model=self._resolve(model),
            messages=messages,  # type: ignore[arg-type]
            stream=True,
        )
        async for chunk in stream:
            if not chunk.choices:
                continue
            delta = chunk.choices[0].delta.content
            if delta:
                yield delta

    async def aclose(self) -> None:
        await self._client.close()
