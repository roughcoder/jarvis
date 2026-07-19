"""Per-engine model catalogs.

The worker owns the model list: it is the tier that actually spawns the provider,
so it is the only one that knows what its local binaries accept. `/health`
publishes the catalog inside `engine_supports.<engine>`, the orchestration tier
mirrors it onto worker/thread payloads, and both tiers validate a requested model
against it before a turn reaches a provider.

Config-driven per AGENTS.md: `WORKER_CODEX_MODELS` / `WORKER_CLAUDE_MODELS` take
a comma-separated `id[:label]` list; empty falls back to the built-ins below.
"""

from __future__ import annotations

from typing import Any

# Fallbacks when the operator has not pinned a list in env. The first entry of an
# engine is its default. These are starting points, not a contract — an operator
# whose binaries speak different names overrides them in `.env`.
DEFAULT_ENGINE_MODELS: dict[str, tuple[tuple[str, str], ...]] = {
    # Verified against a live `codex` install (~/.codex/models_cache.json) — the
    # binary rejects names it does not know, so these are real ids, not guesses.
    "codex": (
        ("gpt-5.6-sol", "GPT-5.6 Sol"),
        ("gpt-5.6-luna", "GPT-5.6 Luna"),
        ("gpt-5.6-terra", "GPT-5.6 Terra"),
        ("gpt-5.5", "GPT-5.5"),
        ("gpt-5.4-mini", "GPT-5.4 Mini"),
    ),
    "claude": (
        ("claude-opus-4-8", "Opus 4.8"),
        ("claude-sonnet-5", "Sonnet 5"),
        ("claude-haiku-4-5-20251001", "Haiku 4.5"),
    ),
}

def parse_model_spec(raw: str) -> list[dict[str, str]]:
    """Parse a `id[:label], id[:label]` env string into catalog rows.

    A bare id gets itself as its label. Blank entries and duplicate ids are
    dropped so a sloppy `.env` line cannot produce a broken picker.
    """
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for entry in str(raw or "").split(","):
        entry = entry.strip()
        if not entry:
            continue
        model_id, _, label = entry.partition(":")
        model_id = model_id.strip()
        label = label.strip()
        if not model_id or model_id in seen:
            continue
        seen.add(model_id)
        rows.append({"id": model_id, "label": label or model_id})
    return rows


def engine_models(cfg: Any, engine: str) -> list[dict[str, str]]:
    """Catalog rows for `engine`, from config with built-in fallback."""
    engine = str(engine or "").strip().lower()
    configured = parse_model_spec(getattr(cfg, f"{engine}_models", "") if engine else "")
    if configured:
        return configured
    return [{"id": model_id, "label": label} for model_id, label in DEFAULT_ENGINE_MODELS.get(engine, ())]


def default_model(cfg: Any, engine: str) -> str:
    """The model a fresh session of `engine` spawns with (first catalog row)."""
    rows = engine_models(cfg, engine)
    return rows[0]["id"] if rows else ""


