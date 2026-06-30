from __future__ import annotations

import re
from collections.abc import Iterable

ENGINE_CODEX = "codex"
ENGINE_CLAUDE = "claude"
BUILTIN_CODE_ENGINES = {ENGINE_CODEX, ENGINE_CLAUDE}


def normalize_engine_id(engine: str) -> str:
    return re.sub(r"[^a-z0-9_.-]+", "-", engine.strip().lower()).strip("-")


def engine_ids(value: str | Iterable[str], *, default_engine: str = ENGINE_CODEX) -> list[str]:
    raw = value.split(",") if isinstance(value, str) else list(value)
    engines: list[str] = []
    for item in raw:
        engine = normalize_engine_id(str(item))
        if engine and engine not in engines:
            engines.append(engine)
    fallback = normalize_engine_id(default_engine) or ENGINE_CODEX
    engines = [engine for engine in engines if engine != fallback]
    engines.insert(0, fallback)
    return engines


def default_engine(engine: str, supported: str | Iterable[str] = ()) -> str:
    return engine_ids(supported, default_engine=engine)[0]


def worker_supports_engine(supported: Iterable[str], engine: str) -> bool:
    return normalize_engine_id(engine) in {normalize_engine_id(x) for x in supported}


def code_engine_argv(
    agent: str,
    codex_bin: str,
    claude_bin: str,
    prompt: str,
    *,
    session_id: str = "",
    session_name: str = "",
    resume_session: bool = False,
) -> list[str]:
    engine = normalize_engine_id(agent) or ENGINE_CODEX
    if engine == ENGINE_CLAUDE:
        argv = [claude_bin]
        if resume_session and session_id:
            return [claude_bin, "-p", "--resume", session_id, prompt]
        if session_id:
            argv.extend(["--session-id", session_id])
        if session_name:
            argv.extend(["--name", session_name])
        argv.extend(["-p", prompt])
        return argv
    if engine == ENGINE_CODEX:
        if session_id:
            return [codex_bin, "exec", "resume", session_id, prompt]
        return [codex_bin, "exec", prompt]
    raise ValueError(f"unsupported coding engine {agent!r}")
