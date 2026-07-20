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

    assert [row["doc_id"] for row in project_file_rows(cfg, "jarvis")] == ["notes", "spec-v2", "draft"]
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
    assert Path(row["original_path"]).name == f"{row['doc_id']}.md"
    assert filtered["query"] == "alpha"

    # And the stored filename is exactly what an @-mention resolves against.
    mentioned = resolve_project_mentions(service.cfg, "jarvis", f"read @{Path(row['original_path']).name}")
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
