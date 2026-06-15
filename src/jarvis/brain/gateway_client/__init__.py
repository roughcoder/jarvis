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
        self, messages: list[dict], *, model: str | None = None, usage_out: dict | None = None
    ) -> AsyncIterator[str]:
        """Streaming completion: yields text deltas for time-to-first-token. When
        `usage_out` is given, request usage and fill it with prompt-cache stats from
        the final chunk (for cache hit/miss tracing, §9)."""
        kwargs: dict = {
            "model": self._resolve(model),
            "messages": messages,
            "stream": True,
            "user": self._speaker,
            "extra_body": self._extra_body,
        }
        if usage_out is not None:
            kwargs["stream_options"] = {"include_usage": True}
        stream = await self._client.chat.completions.create(**kwargs)
        async for chunk in stream:
            if usage_out is not None and getattr(chunk, "usage", None) is not None:
                usage_out.update(_usage_dict(chunk.usage))
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
        usage_out: dict | None = None,
    ):
        """One tool-aware completion. Returns the assistant message (which carries
        `.content` and `.tool_calls`) so the caller can run the tool loop. Tools
        are omitted entirely when none are offered (a plain completion). When
        `usage_out` is given, it's filled with prompt-cache stats (§9)."""
        kwargs: dict = {
            "model": self._resolve(model),
            "messages": messages,
            "user": self._speaker,
            "extra_body": self._extra_body,
        }
        if tools:
            kwargs["tools"] = tools
        resp = await self._client.chat.completions.create(**kwargs)
        if usage_out is not None:
            usage_out.update(_usage_dict(getattr(resp, "usage", None)))
        return resp.choices[0].message

    async def embed(self, texts: list[str], *, model: str | None = None) -> list[list[float]]:
        """Embed texts via the LiteLLM embeddings route (optional tool-relevance scorer)."""
        resp = await self._client.embeddings.create(
            model=model or self._cfg.embed_model, input=texts
        )
        return [d.embedding for d in resp.data]

    async def aclose(self) -> None:
        await self._client.close()


def _usage_dict(usage) -> dict:  # noqa: ANN001
    """Normalise an OpenAI/LiteLLM usage object to a small dict, including cached
    (prompt-cache) tokens however the provider reports them."""
    if usage is None:
        return {}
    out = {
        "prompt_tokens": getattr(usage, "prompt_tokens", None),
        "completion_tokens": getattr(usage, "completion_tokens", None),
    }
    details = getattr(usage, "prompt_tokens_details", None)
    cached = None
    if details is not None:
        cached = details.get("cached_tokens") if isinstance(details, dict) else getattr(details, "cached_tokens", None)
    if cached is None:  # Anthropic-style (via LiteLLM)
        cached = getattr(usage, "cache_read_input_tokens", None)
    out["cached_tokens"] = cached or 0
    return {k: v for k, v in out.items() if v is not None}
