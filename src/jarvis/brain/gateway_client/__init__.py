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
        # Authenticate with the voice virtual key (falls back to master) so the
        # gateway logs attribute voice calls to the "jarvis-voice" key alias.
        key = cfg.client_key.get_secret_value() or cfg.api_key.get_secret_value()
        # base_url points at the LiteLLM proxy; /v1 is the OpenAI-compatible path.
        self._client = AsyncOpenAI(
            base_url=f"{cfg.base_url}/v1",
            api_key=key,
            timeout=cfg.request_timeout_s,
        )
        self._speaker = cfg.speaker  # End User attribution (who's talking)
        # Room attached as a LiteLLM tag so multi-instance traffic is separable.
        self._extra_body = {"metadata": {"tags": [f"room:{cfg.room}"]}}

    def _resolve(self, model: str | None) -> str:
        # Default to the fast route; callers pass cfg.strong_model when needed.
        return model or self._cfg.fast_model

    async def complete(self, messages: list[dict], *, model: str | None = None) -> str:
        """Non-streaming completion. `model` is a LiteLLM route name."""
        resp = await self._client.chat.completions.create(
            model=self._resolve(model),
            messages=messages,  # type: ignore[arg-type]
            user=self._speaker,
            extra_body=self._extra_body,
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
            user=self._speaker,
            extra_body=self._extra_body,
        )
        async for chunk in stream:
            if not chunk.choices:
                continue
            delta = chunk.choices[0].delta.content
            if delta:
                yield delta

    async def complete_with_tools(
        self,
        messages: list[dict],
        *,
        model: str | None = None,
        tools: list[dict] | None = None,
    ):
        """One tool-aware completion. Returns the assistant message (which carries
        `.content` and `.tool_calls`) so the caller can run the tool loop. Tools
        are omitted entirely when none are offered (a plain completion)."""
        kwargs: dict = {
            "model": self._resolve(model),
            "messages": messages,
            "user": self._speaker,
            "extra_body": self._extra_body,
        }
        if tools:
            kwargs["tools"] = tools
        resp = await self._client.chat.completions.create(**kwargs)
        return resp.choices[0].message

    async def aclose(self) -> None:
        await self._client.close()
