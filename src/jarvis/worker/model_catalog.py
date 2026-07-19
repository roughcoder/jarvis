"""Per-engine model, reasoning-effort and speed catalogs.

The worker owns these lists: it is the tier that actually spawns the provider,
so it is the only one that knows what its local binaries accept. `/health`
publishes them inside `engine_supports.<engine>`, the orchestration tier mirrors
them onto worker/thread payloads, and both tiers validate a requested value
against them before a turn reaches a provider.

Config-driven per AGENTS.md: `WORKER_CODEX_MODELS` / `WORKER_CLAUDE_MODELS`,
`WORKER_CODEX_EFFORTS` / `WORKER_CLAUDE_EFFORTS` and `WORKER_CODEX_SPEEDS` /
`WORKER_CLAUDE_SPEEDS` each take a comma-separated `id[:label[:description]]`
list; empty falls back to the built-ins below.
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

# Reasoning-effort levels, as `(id, label, description)`.
#
# codex: verified against the live model cache (~/.codex/models_cache.json,
# `supported_reasoning_levels`). Every model there supports low/medium/high/
# xhigh; `max` and `ultra` exist only on the newest few, so they stay out of a
# per-engine catalog that must hold for whichever model the session runs — an
# operator on a max-capable model adds them via WORKER_CODEX_EFFORTS.
# claude: the SDK's `ClaudeAgentOptions.effort` literal (and `claude --effort`).
DEFAULT_ENGINE_EFFORTS: dict[str, tuple[tuple[str, str, str], ...]] = {
    "codex": (
        ("low", "Light", "Fast responses with lighter reasoning"),
        ("medium", "Medium", "Balances speed and reasoning depth for everyday tasks"),
        ("high", "High", "Greater reasoning depth for complex problems"),
        ("xhigh", "Extra High", "Consumes usage limits faster"),
    ),
    "claude": (
        ("low", "Light", "Fast responses with lighter reasoning"),
        ("medium", "Medium", "Balances speed and reasoning depth for everyday tasks"),
        ("high", "High", "Greater reasoning depth for complex problems"),
        ("xhigh", "Extra High", "Consumes usage limits faster"),
        ("max", "Max", "Maximum reasoning depth for the hardest problems"),
    ),
}

# codex's implicit tier: selecting it means sending no serviceTier at all.
CODEX_STANDARD_SERVICE_TIER = "standard"

# Speed / service tiers. codex's cache advertises one *additional* tier
# (`priority`, "Fast", "1.5x speed, increased usage"); "standard" is the
# implicit default the binary uses when no serviceTier is sent, so it is listed
# here but never transmitted. Claude has no fast mode — an engine with no speeds
# publishes `[]` and the picker hides the row.
DEFAULT_ENGINE_SPEEDS: dict[str, tuple[tuple[str, str, str], ...]] = {
    "codex": (
        ("standard", "Standard", "Default speed"),
        ("priority", "Fast", "1.5x speed, more usage"),
    ),
    "claude": (),
}

# The value a fresh session runs at when the caller names none. Deliberately not
# "first row wins" (as models do): the natural picker order is ascending effort,
# but the useful default sits in the middle of it.
DEFAULT_ENGINE_EFFORT: dict[str, str] = {"codex": "high", "claude": "high"}
DEFAULT_ENGINE_SPEED: dict[str, str] = {"codex": "standard", "claude": ""}

def parse_catalog_spec(raw: str) -> list[dict[str, str]]:
    """Parse an `id[:label[:description]], ...` env string into catalog rows.

    A bare id gets itself as its label. A description is only carried when the
    entry supplies one, so model rows keep their `{id, label}` shape. Blank
    entries and duplicate ids are dropped so a sloppy `.env` line cannot produce
    a broken picker.
    """
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for entry in str(raw or "").split(","):
        entry = entry.strip()
        if not entry:
            continue
        row_id, _, rest = entry.partition(":")
        label, _, description = rest.partition(":")
        row_id = row_id.strip()
        label = label.strip()
        description = description.strip()
        if not row_id or row_id in seen:
            continue
        seen.add(row_id)
        row = {"id": row_id, "label": label or row_id}
        if description:
            row["description"] = description
        rows.append(row)
    return rows


# The models-only name predates efforts/speeds; kept so existing callers and
# tests keep reading naturally.
parse_model_spec = parse_catalog_spec


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


def _catalog(
    cfg: Any,
    engine: str,
    field: str,
    builtins: dict[str, tuple[tuple[str, str, str], ...]],
) -> list[dict[str, str]]:
    """Rows for `engine`'s `field` catalog, from config with built-in fallback."""
    engine = str(engine or "").strip().lower()
    configured = parse_catalog_spec(getattr(cfg, f"{engine}_{field}", "") if engine else "")
    if configured:
        return configured
    return [
        {"id": row_id, "label": label, "description": description}
        for row_id, label, description in builtins.get(engine, ())
    ]


def engine_efforts(cfg: Any, engine: str) -> list[dict[str, str]]:
    """Reasoning-effort rows for `engine`."""
    return _catalog(cfg, engine, "efforts", DEFAULT_ENGINE_EFFORTS)


def engine_speeds(cfg: Any, engine: str) -> list[dict[str, str]]:
    """Speed / service-tier rows for `engine` (empty when it has no fast mode)."""
    return _catalog(cfg, engine, "speeds", DEFAULT_ENGINE_SPEEDS)


def _default_row(rows: list[dict[str, str]], preferred: str) -> str:
    """`preferred` when the catalog still offers it, else the first row.

    An operator who overrides the catalog in env rarely also restates the
    default, so falling back to "first row wins" keeps their list coherent.
    """
    ids = [row["id"] for row in rows]
    if preferred and preferred in ids:
        return preferred
    return ids[0] if ids else ""


def default_effort(cfg: Any, engine: str) -> str:
    """The reasoning effort a fresh session of `engine` runs at."""
    engine = str(engine or "").strip().lower()
    return _default_row(engine_efforts(cfg, engine), DEFAULT_ENGINE_EFFORT.get(engine, ""))


def default_speed(cfg: Any, engine: str) -> str:
    """The speed a fresh session of `engine` runs at ("" when it has none)."""
    engine = str(engine or "").strip().lower()
    return _default_row(engine_speeds(cfg, engine), DEFAULT_ENGINE_SPEED.get(engine, ""))


