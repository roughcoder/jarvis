"""@-mention resolution for project (memory) files.

Covers handle detection, bounded injection, the binary/unreadable fallbacks,
unresolvable passthrough, the `?query=` filter, and both turn paths.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from jarvis.brain.project_files import find_mentions, resolve_project_mentions
from jarvis.brain.project_management import project_file_rows
from jarvis.config import Config


def _cfg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **extra: str) -> Config:
    env = tmp_path / ".env"
    lines = [
        f"REGISTRY_PATH={tmp_path / 'registry.json'}",
        f"REGISTRY_FILES_VAULT_ROOT={tmp_path / 'vault'}",
        f"REGISTRY_UPLOAD_MANIFEST_PATH={tmp_path / 'upload-manifest.json'}",
        *(f"{key}={value}" for key, value in extra.items()),
    ]
    env.write_text("\n".join(lines), encoding="utf-8")
    monkeypatch.setenv("JARVIS_ENV_FILE", str(env))
    return Config()


def _manifest(cfg: Config, rows: list[dict[str, Any]], project_id: str = "jarvis") -> None:
    path = Path(cfg.registry.upload_manifest_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"version": 1, "projects": {project_id: rows}}), encoding="utf-8")


def _row(doc_id: str, path: Path, **extra: Any) -> dict[str, Any]:
    return {
        "doc_id": doc_id,
        "title": extra.pop("title", doc_id),
        "original_path": str(path),
        "mime_type": extra.pop("mime_type", "text/markdown"),
        "retracted": False,
        "observed_at": extra.pop("observed_at", "2026-07-20T10:00:00Z"),
        **extra,
    }


# --- handle detection -------------------------------------------------------


def test_find_mentions_covers_both_forms_and_whitespace_rule() -> None:
    assert find_mentions("read @spec.md please") == ["spec.md"]
    assert find_mentions("read @memory:spec-a1b2c3") == ["memory:spec-a1b2c3"]
    # A handle stops at whitespace, so a name containing one is never a mention.
    assert find_mentions("read @my file.md") == ["my"]
    # Trailing sentence punctuation is not part of the handle; a dot is.
    assert find_mentions("check @spec.md, then @notes.txt.") == ["spec.md", "notes.txt."]
    # De-duplicated, order preserved.
    assert find_mentions("@a.md and @b.md and @a.md") == ["a.md", "b.md"]
    # Email-ish text has no bare `@` handle to grab.
    assert find_mentions("mail neil@eat.sleep.dev") == []
    assert find_mentions("no mentions here") == []


# --- injection --------------------------------------------------------------


def test_mention_injects_bounded_block_for_both_handle_forms(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    doc = tmp_path / "spec.md"
    doc.write_text("# Spec\nthe contents", encoding="utf-8")
    _manifest(cfg, [_row("spec-a1b2c3", doc)])

    by_name = resolve_project_mentions(cfg, "jarvis", "summarise @spec.md")
    assert by_name.startswith("summarise @spec.md")
    assert "--- @file spec.md (project file) ---" in by_name
    assert "the contents" in by_name

    by_doc_id = resolve_project_mentions(cfg, "jarvis", "summarise @memory:spec-a1b2c3")
    assert "the contents" in by_doc_id


def test_mention_content_is_truncated_at_the_configured_cap(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, REGISTRY_MENTION_CONTENT_MAX_BYTES="64")
    doc = tmp_path / "big.md"
    doc.write_text("x" * 500, encoding="utf-8")
    _manifest(cfg, [_row("big", doc)])

    resolved = resolve_project_mentions(cfg, "jarvis", "read @big.md")
    assert "[truncated at 64 bytes of 500]" in resolved
    # The cap is a byte cap on the injected content, not a suggestion.
    assert "x" * 65 not in resolved


def test_mention_file_count_is_capped_per_turn(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, REGISTRY_MENTION_MAX_FILES="2")
    rows = []
    for index in range(4):
        doc = tmp_path / f"f{index}.md"
        doc.write_text(f"body-{index}", encoding="utf-8")
        rows.append(_row(f"f{index}", doc))
    _manifest(cfg, rows)

    resolved = resolve_project_mentions(cfg, "jarvis", "@f0.md @f1.md @f2.md @f3.md")
    assert resolved.count("(project file)") == 2
    assert "body-0" in resolved and "body-1" in resolved
    assert "body-2" not in resolved and "body-3" not in resolved


def test_binary_and_unreadable_files_inject_metadata_not_bytes(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    binary = tmp_path / "logo.png"
    binary.write_bytes(b"\x89PNG\r\n\x1a\n\xff\xfe")
    missing = tmp_path / "gone.md"
    _manifest(
        cfg,
        [
            _row("logo", binary, mime_type="image/png"),
            _row("gone", missing),
        ],
    )

    resolved = resolve_project_mentions(cfg, "jarvis", "look at @logo.png and @gone.md")
    assert "[binary file: logo.png, image/png, 10 bytes — content not inlined]" in resolved
    assert "PNG" not in resolved.replace("logo.png", "")
    assert "[unavailable: gone.md (text/markdown) could not be read]" in resolved


def test_unresolvable_and_retracted_mentions_pass_through(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    doc = tmp_path / "old.md"
    doc.write_text("stale", encoding="utf-8")
    _manifest(cfg, [_row("old", doc, retracted=True)])

    for text in ("ping @nope.md", "ping @memory:nope", "ping @old.md", "no mentions"):
        assert resolve_project_mentions(cfg, "jarvis", text) == text


def test_corrupt_manifest_never_breaks_the_turn(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    path = Path(cfg.registry.upload_manifest_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not json", encoding="utf-8")

    assert resolve_project_mentions(cfg, "jarvis", "read @spec.md") == "read @spec.md"


def test_missing_project_id_is_a_no_op(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    assert resolve_project_mentions(cfg, "", "read @spec.md") == "read @spec.md"


# --- picker query -----------------------------------------------------------


def test_project_file_rows_query_filters_ranks_and_caps(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _manifest(
        cfg,
        [
            _row("notes", tmp_path / "notes.md", title="Meeting notes", observed_at="2026-07-01"),
            _row("spec-v2", tmp_path / "spec-v2.md", title="Spec v2", observed_at="2026-07-02"),
            _row("draft", tmp_path / "draft.md", title="A spec draft", observed_at="2026-07-03"),
            _row("gone", tmp_path / "gone.md", retracted=True),
        ],
    )

    rows = project_file_rows(cfg, "jarvis")
    assert [row["doc_id"] for row in rows] == ["notes", "spec-v2", "draft"]
    # Every row carries the stored filename the composer turns into a handle,
    # so the picker never has to parse a brain-host path.
    assert [row["filename"] for row in rows] == ["notes.md", "spec-v2.md", "draft.md"]
    assert all(row["filename"] == Path(row["original_path"]).name for row in rows)
    # A handle must never contain whitespace, or it cannot be mentioned at all.
    assert all(" " not in row["filename"] for row in rows)
    # Filename prefix ("spec-v2.md") outranks the title substring hit ("A spec draft").
    assert [row["doc_id"] for row in project_file_rows(cfg, "jarvis", query="spec")] == ["spec-v2", "draft"]
    assert project_file_rows(cfg, "jarvis", query="zzz") == []
    assert len(project_file_rows(cfg, "jarvis", query="e", limit=1)) == 1
    assert [row["doc_id"] for row in project_file_rows(cfg, "jarvis", include_retracted=True, query="gone")] == ["gone"]


def test_file_list_op_accepts_a_query(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from test_project_management import _service

    service, _store, _memory = _service(tmp_path, monkeypatch)
    from conftest import request_context

    ctx = request_context(identity="neil", scope="personal", channel="cockpit", peer="neil")
    for name in ("alpha.md", "beta.md"):
        asyncio.run(
            service.execute(
                ctx,
                "project.file.upload",
                {"project_id": "jarvis", "filename": name, "content_text": f"body of {name}"},
            )
        )

    everything = asyncio.run(service.execute(ctx, "project.file.list", {"project_id": "jarvis"}))
    assert len(everything["files"]) == 2
    assert everything["query"] == ""

    filtered = asyncio.run(service.execute(ctx, "project.file.list", {"project_id": "jarvis", "query": "alpha"}))
    # Uploads get a content-hash-suffixed doc_id ("alpha-<hash12>"); the stored
    # filename is that plus the original suffix, and both match the query.
    assert len(filtered["files"]) == 1
    row = filtered["files"][0]
    assert row["doc_id"].startswith("alpha-")
    assert row["filename"] == f"{row['doc_id']}.md"
    assert row["filename"] == Path(row["original_path"]).name
    assert filtered["query"] == "alpha"

    # The contract the composer depends on: the row's `filename` is exactly the
    # handle the resolver matches — build `@<filename>` and it resolves.
    mentioned = resolve_project_mentions(service.cfg, "jarvis", f"read @{row['filename']}")
    assert "body of alpha.md" in mentioned


# --- turn paths -------------------------------------------------------------


def test_project_thread_turn_injects_mentions_but_persists_text_as_typed(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    """Orchestrator/project-thread path: the provider sees the file content,
    the transcript keeps what the user actually typed."""
    from jarvis.brain.facade import ProjectEntry, RequestContext
    from jarvis.connectors.cockpit import CockpitConnector
    from test_cockpit_connector import FakeMemory

    seen: dict[str, str] = {}

    class StubSession:
        def __init__(self) -> None:
            self.pending_cold_tasks: tuple[Any, ...] = ()

        async def respond_text(self, text, _trace, result, *, attachments=None, on_text=None):  # noqa: ANN001
            seen["prompt"] = text
            result.raw = "Read it."
            return result.raw

        def finalize(self, text, result, _trace) -> None:  # noqa: ANN001
            seen["finalized"] = text
            result.reply = result.raw

    env = tmp_path / ".env"
    env.write_text(
        "\n".join(
            [
                f"ORCHESTRATION_WORKSPACE={tmp_path / 'orchestration'}",
                f"REGISTRY_PATH={tmp_path / 'registry.json'}",
                f"REGISTRY_UPLOAD_MANIFEST_PATH={tmp_path / 'upload-manifest.json'}",
                f"MEMORY_CACHE_PATH={tmp_path / 'memory-cache.json'}",
                f"MEMORY_CURATION_OUTBOX_PATH={tmp_path / 'outbox.jsonl'}",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("JARVIS_ENV_FILE", str(env))
    cfg = Config()

    doc = tmp_path / "spec.md"
    doc.write_text("SPEC BODY", encoding="utf-8")
    _manifest(cfg, [_row("spec", doc)])

    async def verify() -> None:
        connector = CockpitConnector(cfg, memory=FakeMemory(), gateway=object(), tts=None, tracer=None)
        project = ProjectEntry(id="jarvis", name="Jarvis", owner="neil", members=("neil",), visibility="private")
        requester = RequestContext("dev", "neil", "personal", frozenset(), channel="cockpit", peer="neil")
        thread = await connector.open_thread(project, requester, title="Planning")
        monkeypatch.setattr(connector, "_make_session", lambda *_a, **_k: StubSession())

        stored = connector._index.get(project.id, thread.thread_id)
        _reply, updated, _events = await connector.turn(project, stored, requester, "summarise @spec.md")

        # The provider got the injected block...
        assert "SPEC BODY" in seen["prompt"]
        assert "--- @file spec.md (project file) ---" in seen["prompt"]
        # ...and the durable transcript kept the mention as typed.
        assert seen["finalized"] == "summarise @spec.md"
        user_messages = [row for row in updated.messages if row.get("role") == "user"]
        assert user_messages[-1]["content"] == "summarise @spec.md"

    asyncio.run(verify())


def test_worker_session_turn_resolves_mentions_before_the_worker_hop(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    """Worker-session path: files live brain-side, so only the rendered prompt
    crosses the HTTP boundary to the worker."""
    from jarvis.orchestration.api import _session_turn_with_mentions
    from jarvis.orchestration.models import OrchestrationRun, WorkerSessionLink

    cfg = _cfg(tmp_path, monkeypatch)
    doc = tmp_path / "plan.md"
    doc.write_text("PLAN BODY", encoding="utf-8")
    _manifest(cfg, [_row("plan", doc)])

    class StubStore:
        def list_runs(self) -> list[OrchestrationRun]:
            run = OrchestrationRun(run_id="run_1", objective="ship it", project_id="jarvis")
            run.sessions = [WorkerSessionLink(worker_id="w1", session_id="s1", project_id="jarvis")]
            return [run]

    class StubCtx:
        def __init__(self) -> None:
            self.cfg = cfg
            self.store = StubStore()

    class Ref:
        worker_id = "w1"
        session_id = "s1"

    async def verify() -> None:
        ctx, ref = StubCtx(), Ref()

        resolved = await _session_turn_with_mentions(ctx, ref, {"prompt": "apply @plan.md"})
        assert "PLAN BODY" in resolved["prompt"]
        assert resolved["prompt"].startswith("apply @plan.md")

        # Nothing to resolve leaves the body object untouched.
        untouched = {"prompt": "apply @nope.md"}
        assert await _session_turn_with_mentions(ctx, ref, untouched) is untouched
        plain = {"prompt": "no mentions"}
        assert await _session_turn_with_mentions(ctx, ref, plain) is plain

    asyncio.run(verify())


def test_worker_session_turn_without_a_project_link_is_a_no_op(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.api import _session_turn_with_mentions

    cfg = _cfg(tmp_path, monkeypatch)

    class StubCtx:
        def __init__(self) -> None:
            self.cfg = cfg
            self.store = type("S", (), {"list_runs": lambda _self: []})()

    body = {"prompt": "apply @plan.md"}
    assert asyncio.run(_session_turn_with_mentions(StubCtx(), type("R", (), {"worker_id": "w", "session_id": "s"})(), body)) is body


# --- worker-backed thread paths ---------------------------------------------
#
# The two branches of `CockpitConnector.turn()` that hand the prompt to a worker
# session used to persist the text they were given — which is the *injected*
# prompt. That rewrote the user's words in the durable transcript and replayed
# the inlined file as history on every later turn in the thread.


def _worker_thread_connector(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, doc_body: str):  # noqa: ANN202
    """A connector whose project vault holds `@spec.md`, plus its project."""

    from test_cockpit_api import FakeGateway, FakeProjectMemory, _cfg as _api_cfg, _seed_project_registry

    from jarvis.connectors.cockpit import CockpitConnector
    from jarvis.brain.registry import RegistryStore

    cfg = _api_cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None

    doc = tmp_path / "spec.md"
    doc.write_text(doc_body, encoding="utf-8")
    _manifest(cfg, [_row("spec", doc)], project_id=project.id)

    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
    )
    return connector, project


def _persisted_user_messages(connector, project_id: str, thread_id: str) -> list[str]:  # noqa: ANN001
    """Re-read the thread from disk — #138 showed an in-memory assertion can
    pass while the persisted form is wrong."""

    from jarvis.connectors.cockpit import CockpitThreadIndex

    stored = CockpitThreadIndex(connector.index.path).get_with_messages(project_id, thread_id, limit=50)
    assert stored is not None
    return [str(row.get("content") or "") for row in stored.messages if row.get("role") == "user"]


def test_orchestrator_thread_turn_persists_typed_text_not_the_injection(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.brain.facade import RequestContext
    from jarvis.connectors.cockpit import CockpitThread, orchestrator_session_id

    connector, project = _worker_thread_connector(tmp_path, monkeypatch, "SPEC BODY")
    thread = CockpitThread(
        thread_id="thread_orchestrator_mention",
        project_id=project.id,
        session_id=orchestrator_session_id(project.id, "thread_orchestrator_mention"),
        title="Plan",
        created_at="2026-07-20T10:00:00Z",
        updated_at="2026-07-20T10:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        engine="codex",
        workspace={
            "worker_id": "worker_a",
            "session_id": "orch_thread_orchestrator_mention",
            "provider_started": True,
            "status": "ready",
            "session_generation": 1,
        },
    )
    connector.index.save(thread)

    posted: list[dict[str, Any]] = []

    async def ensure(_project, current_thread, _requester, **_kwargs):  # noqa: ANN001, ANN003
        return current_thread

    def post(_worker_id: str, _path: str, body: dict[str, Any]) -> dict[str, Any]:
        posted.append(body)
        return {"ok": True}

    async def wait(*_args: Any) -> str:
        return "Read it."

    monkeypatch.setattr(connector, "_ensure_orchestrator_session", ensure)
    monkeypatch.setattr(connector, "_post_worker_json", post)
    monkeypatch.setattr(connector, "_wait_for_orchestrator_turn", wait)

    typed = "summarise @spec.md"
    reply, _updated, _events = asyncio.run(
        connector.turn(project, thread, RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil"), typed)
    )

    assert reply == "Read it."
    # The provider got the injected block...
    assert "SPEC BODY" in posted[0]["prompt"]
    assert "--- @file spec.md (project file) ---" in posted[0]["prompt"]
    # ...and the durable transcript kept only what the user typed.
    persisted = _persisted_user_messages(connector, project.id, thread.thread_id)
    assert persisted == [typed]


def test_workspace_thread_turn_persists_typed_text_not_the_injection(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.brain.facade import RequestContext
    from jarvis.connectors.cockpit import CockpitThread

    connector, project = _worker_thread_connector(tmp_path, monkeypatch, "SPEC BODY")
    thread = CockpitThread(
        thread_id="thread_workspace_mention",
        project_id=project.id,
        session_id=f"project:{project.id}:thread_workspace_mention",
        title="Build",
        created_at="2026-07-20T10:00:00Z",
        updated_at="2026-07-20T10:00:00Z",
        created_by="neil",
        workspace={
            "worker_id": "worker_a",
            "session_id": "conv_thread_workspace_mention",
            "status": "ready",
            "session_generation": 1,
        },
    )
    connector.index.save(thread)

    posted: list[dict[str, Any]] = []

    async def ensure(_project, current_thread, _requester, **_kwargs):  # noqa: ANN001, ANN003
        return current_thread

    def post(_worker_id: str, _path: str, body: dict[str, Any]) -> dict[str, Any]:
        posted.append(body)
        return {"ok": True}

    monkeypatch.setattr(connector, "_ensure_workspace", ensure)
    monkeypatch.setattr(connector, "_post_worker_json", post)

    typed = "apply @spec.md"
    _reply, _updated, _events = asyncio.run(
        connector.turn(project, thread, RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil"), typed)
    )

    assert "SPEC BODY" in posted[0]["prompt"]
    assert "--- @file spec.md (project file) ---" in posted[0]["prompt"]
    persisted = _persisted_user_messages(connector, project.id, thread.thread_id)
    assert persisted == [typed]
