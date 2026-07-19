"""Memory-as-tool and Lane 2 curation tools."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from jarvis.brain.capabilities import can_query_memory_peer, can_write_memory_peer
from jarvis.brain.memory_client import ConclusionRecord, MemoryBackend
from jarvis.brain.memory_outbox import (
    ActiveRetraction,
    CurationOutbox,
    RetractionIndex,
    memory_text_matches,
    retraction_index_path,
)
from jarvis.brain.registry import RegistryStore
from jarvis.config import MemoryConfig
from jarvis.runtime import RequestContext
from jarvis.tools.base import Tool
from jarvis.users import User

QUERY_CAPABILITY = "memory.query"
CURATE_CAPABILITY = "memory.curate"
DERIVED_CONCLUSION_LEVELS = {"deductive", "inductive"}
MEMORY_RETRACTION_INSTRUCTION = (
    "Memory note: the withdrawals below are authoritative. Do not present a "
    "withdrawn fact as current, including semantically equivalent restatements "
    "that Honcho may have re-derived with different wording."
)


def make_memory_tools(
    cfg: MemoryConfig,
    *,
    memory: MemoryBackend,
    outbox: CurationOutbox,
    registry: RegistryStore,
    users: dict[str, User] | None = None,
) -> list[Tool]:
    users = users or {}
    retractions = RetractionIndex(retraction_index_path(outbox.path))

    async def memory_search(ctx: RequestContext, args: dict[str, Any]) -> str:
        search_query = (args.get("search_query") or args.get("query") or "").strip()
        target = (args.get("target") or "").strip()
        peer_id = _resolve_peer(target, ctx, registry, users)
        decision = can_query_memory_peer(ctx, peer_id, registry=registry, users=users)
        if not decision.allowed:
            return f"error: {decision.reason}"
        cached = memory.read_cached_representation(peer_id)
        active_retractions = retractions.active(observed_id=peer_id)
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    memory.read_representation,
                    peer_id,
                    search_query=search_query or None,
                    target=ctx.memory_peer if ctx.memory_peer != peer_id else None,
                ),
                timeout=cfg.tool_timeout_s,
            )
            text = result.representation.strip()
            if search_query:
                answer = await asyncio.wait_for(
                    asyncio.to_thread(
                        memory.dialectic_chat,
                        peer_id,
                        search_query,
                        target=ctx.memory_peer if ctx.memory_peer != peer_id else None,
                    ),
                    timeout=cfg.tool_timeout_s,
                )
                if answer.strip():
                    text = answer.strip()
            text = outbox.append_pending_lines(text or "No memory found.", observed_id=peer_id)
            return _memory_tool_result(text, retractions=active_retractions)
        except Exception as exc:  # noqa: BLE001 - memory tool degrades, never breaks a turn.
            fallback = cached or "No cached memory found."
            fallback = outbox.append_pending_lines(fallback, observed_id=peer_id)
            return _memory_tool_result(
                f"{fallback}\nmemory is unreachable: {exc}",
                retractions=active_retractions,
            )

    async def remember_contact(ctx: RequestContext, args: dict[str, Any]) -> str:
        contact_name = (args.get("contact") or args.get("person") or args.get("name") or "").strip()
        fact = (args.get("fact") or args.get("content") or "").strip()
        if not contact_name or not fact:
            return "error: provide both contact/person and fact."
        resolution = registry.resolve_contact(contact_name, ctx.identity)
        if resolution.status == "ambiguous":
            return "confirmation required: which contact? " + ", ".join(resolution.speakable_names)
        if resolution.status == "not_found" or resolution.entry is None:
            return (
                "confirmation required: create a contact before saving this memory "
                f"(name={contact_name!r})."
            )
        decision = can_write_memory_peer(ctx, resolution.entry.peer_id, registry=registry)
        if not decision.allowed:
            return f"error: {decision.reason}"
        metadata = _base_metadata(ctx, args)
        entry = outbox.enqueue_create(
            observed_id=resolution.entry.peer_id,
            observer_id=ctx.memory_peer,
            content=fact,
            metadata=metadata,
        )
        return f"Noted - queued memory for {resolution.entry.display_name} ({entry.content_hash})."

    async def add_finding(ctx: RequestContext, args: dict[str, Any]) -> str:
        return _queue_project_artifact(ctx, args, artifact_type="finding")

    async def record_decision(ctx: RequestContext, args: dict[str, Any]) -> str:
        return _queue_project_artifact(ctx, args, artifact_type="decision")

    def _queue_project_artifact(
        ctx: RequestContext,
        args: dict[str, Any],
        *,
        artifact_type: str,
    ) -> str:
        project_name = (args.get("project") or args.get("project_id") or "").strip()
        content = (args.get("content") or args.get(artifact_type) or "").strip()
        if not project_name or not content:
            return f"error: provide both project and {artifact_type} content."
        resolution = registry.resolve_project(project_name, ctx.identity)
        if resolution.status == "ambiguous":
            return "confirmation required: which project? " + ", ".join(resolution.speakable_names)
        if resolution.status == "not_found" or resolution.entry is None:
            return "error: project not found or not visible."
        decision = can_write_memory_peer(ctx, resolution.entry.peer_id, registry=registry)
        if not decision.allowed:
            return f"error: {decision.reason}"
        status = (args.get("status") or ("accepted" if artifact_type == "decision" else "open")).strip()
        metadata = {
            **_base_metadata(ctx, args),
            "project_id": resolution.entry.id,
            "artifact_type": artifact_type,
            "status": status,
        }
        entry = outbox.enqueue_create(
            observed_id=resolution.entry.peer_id,
            observer_id=ctx.memory_peer,
            content=content,
            metadata=metadata,
        )
        return f"Noted - queued {artifact_type} for {resolution.entry.name} ({entry.content_hash})."

    async def forget_memory(ctx: RequestContext, args: dict[str, Any]) -> str:
        return await _forget_or_correct(ctx, args, replacement="")

    async def correct_memory(ctx: RequestContext, args: dict[str, Any]) -> str:
        replacement = (args.get("replacement") or args.get("corrected") or "").strip()
        if not replacement:
            return "error: provide replacement text for the correction."
        return await _forget_or_correct(ctx, args, replacement=replacement)

    async def _forget_or_correct(
        ctx: RequestContext,
        args: dict[str, Any],
        *,
        replacement: str,
    ) -> str:
        query = (args.get("query") or args.get("search_query") or "").strip()
        if not query:
            return "error: provide the memory to search for."
        peer_id = _resolve_peer((args.get("target") or "").strip(), ctx, registry, users)
        decision = can_write_memory_peer(ctx, peer_id, registry=registry)
        if not decision.allowed:
            return f"error: {decision.reason}"
        cancelled = outbox.cancel_pending(observed_id=peer_id, content=query)
        if cancelled:
            if replacement:
                metadata = _base_metadata(ctx, args)
                outbox.enqueue_create(
                    observed_id=peer_id,
                    observer_id=ctx.memory_peer,
                    content=replacement,
                    metadata=metadata,
                )
                retractions.clear_for_assertion(observed_id=peer_id, content=replacement)
                return "Corrected."
            return "Forgotten."
        confirmed = bool(args.get("confirm"))
        ids = [str(item).strip() for item in args.get("conclusion_ids", []) if str(item).strip()]
        if not confirmed:
            matches = await asyncio.to_thread(
                memory.query_conclusions,
                query,
                observed_id=peer_id,
                limit=5,
            )
            return _confirmation_text(matches, replacement=bool(replacement))
        if not ids:
            return "error: confirmation requires conclusion_ids."
        matches = await asyncio.to_thread(memory.list_conclusions, observed_id=peer_id)
        selected, missing = _selected_conclusions(matches, ids)
        if missing:
            return "error: selected memories were not found; search again before confirming."
        for conclusion_id in ids:
            outbox.enqueue_delete(conclusion_id=conclusion_id, observed_id=peer_id, content=query)
        for conclusion in selected:
            if conclusion.level in DERIVED_CONCLUSION_LEVELS and not _replacement_reasserts_conclusion(
                replacement, conclusion
            ):
                metadata = _retraction_metadata(ctx, args, conclusion)
                # Honcho v3.0.11 stores this as explicit; the local index below is
                # the suppression source, while this queued row remains audit data.
                outbox.enqueue_create(
                    observed_id=peer_id,
                    observer_id=ctx.memory_peer,
                    content=_retraction_content(conclusion),
                    metadata=metadata,
                )
                retractions.record(observed_id=peer_id, metadata=metadata)
        if replacement:
            metadata = _base_metadata(ctx, args)
            outbox.enqueue_create(
                observed_id=peer_id,
                observer_id=ctx.memory_peer,
                content=replacement,
                metadata=metadata,
            )
            retractions.clear_for_assertion(observed_id=peer_id, content=replacement)
            return "Corrected."
        return "Forgotten."

    tools = [
        Tool(
            name="memory_search",
            description=(
                "Search Jarvis memory live for an explicit memory question. Use search_query "
                "for the question and optional target for a visible contact, project, or peer."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "search_query": {"type": "string", "description": "The memory question or semantic filter."},
                    "target": {"type": "string", "description": "Optional contact, project, or peer id to query."},
                },
                "required": ["search_query"],
            },
            required_capability=QUERY_CAPABILITY,
            handler=memory_search,
            announce=True,
            timeout_s=cfg.tool_timeout_s + 1.0,
        ),
    ]
    tools.extend([
        Tool(
            name="remember_contact",
            description=(
                "Save a durable declared fact about a contact/person to Honcho memory. "
                "Use this for 'remember about Klaus...' style requests, not for facts about the current user."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "contact": {"type": "string"},
                    "fact": {"type": "string"},
                    "observed_at": {"type": "string", "description": "ISO date; defaults to today."},
                    "source": {"type": "string"},
                    "channel": {"type": "string"},
                },
                "required": ["contact", "fact"],
            },
            required_capability=CURATE_CAPABILITY,
            handler=remember_contact,
        ),
        Tool(
            name="forget_memory",
            description=(
                "Find a memory semantically, ask for confirmation, then queue deletion "
                "when called again with confirm=true and conclusion_ids."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "target": {"type": "string"},
                    "confirm": {"type": "boolean"},
                    "conclusion_ids": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["query"],
            },
            required_capability=CURATE_CAPABILITY,
            handler=forget_memory,
        ),
        Tool(
            name="correct_memory",
            description=(
                "Find a memory semantically, ask for confirmation, then queue deletion "
                "and a replacement explicit conclusion."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "replacement": {"type": "string"},
                    "target": {"type": "string"},
                    "confirm": {"type": "boolean"},
                    "conclusion_ids": {"type": "array", "items": {"type": "string"}},
                    "observed_at": {"type": "string"},
                },
                "required": ["query", "replacement"],
            },
            required_capability=CURATE_CAPABILITY,
            handler=correct_memory,
        ),
        Tool(
            name="add_finding",
            description="Queue a durable project finding as an explicit conclusion on a visible project peer.",
            parameters={
                "type": "object",
                "properties": {
                    "project": {"type": "string"},
                    "content": {"type": "string"},
                    "status": {"type": "string"},
                    "observed_at": {"type": "string"},
                },
                "required": ["project", "content"],
            },
            required_capability=CURATE_CAPABILITY,
            handler=add_finding,
        ),
        Tool(
            name="record_decision",
            description="Queue a durable project decision as an explicit conclusion on a visible project peer.",
            parameters={
                "type": "object",
                "properties": {
                    "project": {"type": "string"},
                    "content": {"type": "string"},
                    "status": {"type": "string"},
                    "observed_at": {"type": "string"},
                },
                "required": ["project", "content"],
            },
            required_capability=CURATE_CAPABILITY,
            handler=record_decision,
        ),
    ])
    return tools


def _base_metadata(ctx: RequestContext, args: dict[str, Any]) -> dict[str, Any]:
    return {
        "recorded_by": ctx.memory_peer,
        "source": (args.get("source") or "spoken").strip(),
        "channel": (args.get("channel") or ctx.channel or "voice").strip(),
        "observed_at": _observed_at(args),
    }


def _retraction_metadata(
    ctx: RequestContext,
    args: dict[str, Any],
    conclusion: ConclusionRecord,
) -> dict[str, Any]:
    return {
        **_base_metadata(ctx, {**args, "source": "forget"}),
        "level": "contradiction",
        "retracted_conclusion_id": conclusion.id,
        "retracted_conclusion_level": conclusion.level,
        "retracted_content": conclusion.content,
        "retraction_reason": "user_forget_request",
    }


def _retraction_content(conclusion: ConclusionRecord) -> str:
    return (
        "Retraction: the user withdrew this memory and does not want it retained "
        f"as current: {conclusion.content}"
    )


def _observed_at(args: dict[str, Any]) -> str:
    value = (args.get("observed_at") or "").strip()
    return value or datetime.now(UTC).date().isoformat()


def _resolve_peer(
    target: str,
    ctx: RequestContext,
    registry: RegistryStore,
    users: dict[str, User],
) -> str:
    if not target:
        return ctx.memory_peer
    if target.startswith(("contact:", "project:")):
        return target
    if target in users:
        return users[target].peer
    project = registry.resolve_project(target, ctx.identity)
    if project.status == "matched" and project.entry is not None:
        return project.entry.peer_id
    contact = registry.resolve_contact(target, ctx.identity)
    if contact.status == "matched" and contact.entry is not None:
        return contact.entry.peer_id
    return target


def _confirmation_text(matches: list[ConclusionRecord], *, replacement: bool) -> str:
    if not matches:
        return "No matching memories found."
    action = "correct" if replacement else "forget"
    lines = [f"confirmation required: choose conclusion_ids to {action}."]
    for match in matches:
        lines.append(f"- {match.id}: {match.content} (level: {match.level})")
    return "\n".join(lines)


def _selected_conclusions(
    matches: list[ConclusionRecord],
    ids: list[str],
) -> tuple[list[ConclusionRecord], list[str]]:
    by_id = {match.id: match for match in matches}
    selected = [by_id[conclusion_id] for conclusion_id in ids if conclusion_id in by_id]
    missing = [conclusion_id for conclusion_id in ids if conclusion_id not in by_id]
    return selected, missing


def _replacement_reasserts_conclusion(replacement: str, conclusion: ConclusionRecord) -> bool:
    if not replacement:
        return False
    return _text_matches(replacement, conclusion.content)


def _text_matches(left: str, right: str) -> bool:
    return memory_text_matches(left, right)


def _memory_tool_result(text: str, *, retractions: list[ActiveRetraction] | None = None) -> str:
    active = retractions or []
    parts = []
    if active:
        parts.append(MEMORY_RETRACTION_INSTRUCTION)
        parts.append(_retractions_tool_block(active))
    parts.append(text.strip())
    return "\n".join(part for part in parts if part)


def _retractions_tool_block(retractions: list[ActiveRetraction]) -> str:
    lines = ["The user has retracted / withdrawn these memory claims:"]
    for item in retractions:
        suffix = f" (withdrawn {item.observed_at})" if item.observed_at else ""
        lines.append(f"- {item.retracted_content}{suffix}")
    return "\n".join(lines)
