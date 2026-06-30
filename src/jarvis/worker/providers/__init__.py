from __future__ import annotations

from jarvis.worker.providers.base import ProviderAdapter, ProviderTurn
from jarvis.worker.providers.codex import CodexProviderAdapter
from jarvis.worker.providers.claude import ClaudeProviderAdapter
from jarvis.worker.providers.fake import FakeProviderAdapter

__all__ = [
    "ClaudeProviderAdapter",
    "CodexProviderAdapter",
    "FakeProviderAdapter",
    "ProviderAdapter",
    "ProviderTurn",
    "provider_for",
]


def provider_for(provider: str) -> ProviderAdapter:
    name = (provider or "").strip().lower()
    if name == "fake":
        return FakeProviderAdapter()
    if name == "codex":
        return CodexProviderAdapter()
    if name == "claude":
        return ClaudeProviderAdapter()
    raise ValueError(f"unsupported worker session provider {provider!r}")
