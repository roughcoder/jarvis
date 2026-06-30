from __future__ import annotations

import re

from jarvis.engines import normalize_engine_id
from jarvis.orchestration.models import WorkCommand


def parse_work_command(text: str) -> WorkCommand:
    """Deterministic v1 intent mapper.

    The voice/text LLM can emit this structure directly later. CLI and tests use
    this conservative fallback so adapters never parse casual English themselves.
    """
    t = " ".join(text.lower().split())
    source = _source_target(t)
    target = _worker_target(t)
    engine = _engine_target(t)
    filters: dict[str, str] = {}
    if "bug" in t:
        filters["label"] = "bug"
    if "assigned" in t or "my " in t or " me" in t:
        filters["assignee"] = "me"
    if "ready" in t or "next" in t:
        filters["status"] = "ready"

    if "running" in t or "what's running" in t or "whats running" in t:
        return WorkCommand("inspect_runs", source="jarvis", autonomy="read_only", target_worker_id=target, target_engine_id=engine)
    if "resume" in t or "continue" in t:
        return WorkCommand("resume_run", source="jarvis", autonomy="start_if_unambiguous", target_worker_id=target, target_engine_id=engine)
    if "blocked" in t or "stalled" in t:
        return WorkCommand("inspect_blocked", source="jarvis", autonomy="read_only", target_worker_id=target, target_engine_id=engine)
    if "fix" in t or "address" in t or "handle" in t:
        kind = "pull_request" if _mentions_pr(t) or _has_word(t, "review") or _mentions_comment(t) else "issue"
        return WorkCommand("start_selected_work", source=source, kind=kind, filters=filters, autonomy="start_if_unambiguous", start=True, target_worker_id=target, target_engine_id=engine)
    if _mentions_comment(t) and _mentions_pr(t):
        return WorkCommand("inspect_pr_comments", source="github", kind="pull_request", filters=filters, autonomy="read_only", target_worker_id=target, target_engine_id=engine)
    if "get" in t or "take" in t or "pick up" in t or "start" in t or "work on" in t:
        kind = "ticket" if source == "linear" else "issue"
        return WorkCommand("start_next_work", source=source, kind=kind, filters=filters, autonomy="start_if_unambiguous", start=True, target_worker_id=target, target_engine_id=engine)
    if "check" in t or "show" in t or "list" in t or "summarize" in t:
        kind = "ticket" if source == "linear" else "issue"
        if _mentions_pr(t) or _has_word(t, "review"):
            kind = "pull_request"
        return WorkCommand("inspect_work", source=source, kind=kind, filters=filters, autonomy="read_only", target_worker_id=target, target_engine_id=engine)
    if source != "direct":
        kind = "ticket" if source == "linear" else "issue"
        if _mentions_pr(t) or _has_word(t, "review"):
            kind = "pull_request"
        return WorkCommand("inspect_work", source=source, kind=kind, filters=filters, autonomy="read_only", target_worker_id=target, target_engine_id=engine)
    return WorkCommand("direct_request", source="direct", filters={"text": text}, autonomy="read_only", target_worker_id=target, target_engine_id=engine)


def _worker_target(text: str) -> str:
    m = re.search(r"\b(?:on|using|use)\s+([a-z0-9][a-z0-9_-]*-worker)\b", text)
    return m.group(1) if m else ""


def _engine_target(text: str) -> str:
    matches = list(re.finditer(r"\b(?:with|using|via|on)\s+(codex|claude)(?![-_a-z0-9])\b", text))
    return normalize_engine_id(matches[-1].group(1)) if matches else ""


def _source_target(text: str) -> str:
    if _has_word(text, "linear") or _has_word(text, "ticket") or _has_word(text, "tickets"):
        return "linear"
    if (
        _has_word(text, "github")
        or _has_word(text, "issue")
        or _has_word(text, "issues")
        or _mentions_pr(text)
    ):
        return "github"
    return "direct"


def _mentions_pr(text: str) -> bool:
    return bool(re.search(r"\b(?:pr|prs|pull request|pull requests)\b", text))


def _mentions_comment(text: str) -> bool:
    return bool(re.search(r"\bcomments?\b", text))


def _has_word(text: str, word: str) -> bool:
    return bool(re.search(rf"\b{re.escape(word)}\b", text))
