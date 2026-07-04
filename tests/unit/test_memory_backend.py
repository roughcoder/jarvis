from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from jarvis.brain.memory_client import (
    HonchoV2MemoryClient,
    MemoryClient,
    MemoryMessage,
    SessionPeer,
    UnsupportedMemoryOperation,
    decode_honcho_id,
    encode_honcho_id,
)
from jarvis.brain.memory_client.v3 import HonchoV3MemoryClient
from jarvis.config import MemoryConfig


def _cfg(tmp_path: Path, **over: Any) -> MemoryConfig:
    return MemoryConfig(
        _env_file=None,
        backend="v3",
        port=8003,
        workspace_id="jarvis-dev",
        cache_path=str(tmp_path / "representation.json"),
        conclusion_sidecar_path=str(tmp_path / "sidecar.json"),
        **over,
    )


def _json(request: httpx.Request) -> dict[str, Any]:
    if not request.content:
        return {}
    return json.loads(request.content.decode("utf-8"))


def test_memory_client_factory_keeps_v2_default() -> None:
    client = MemoryClient(MemoryConfig(_env_file=None))

    assert isinstance(client, HonchoV2MemoryClient)
    with pytest.raises(UnsupportedMemoryOperation):
        client.list_conclusions()


def test_memory_client_factory_selects_v3(tmp_path) -> None:
    client = MemoryClient(_cfg(tmp_path))

    assert isinstance(client, HonchoV3MemoryClient)


def test_honcho_id_encoding_is_reversible_and_keeps_safe_ids() -> None:
    assert encode_honcho_id("jarvis-dev") == "jarvis-dev"

    for jarvis_id in ("voice:neil:mac", "project:jarvis", "contact:klaus", "jv_native"):
        encoded = encode_honcho_id(jarvis_id)
        assert ":" not in encoded
        assert decode_honcho_id(encoded) == jarvis_id


def test_v3_create_messages_sets_session_membership_before_writing(tmp_path) -> None:
    calls: list[tuple[str, str, dict[str, Any]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path, _json(request)))
        if request.url.path.endswith("/messages"):
            return httpx.Response(201, json=[{"id": "m1"}, {"id": "m2"}])
        return httpx.Response(201, json={"id": _json(request).get("id", "")})

    client = HonchoV3MemoryClient(_cfg(tmp_path), transport=httpx.MockTransport(handler))
    created = client.create_messages(
        "voice:neil:mac",
        [
            MemoryMessage("neil", "hello", {"channel": "voice"}),
            MemoryMessage("jarvis", "hi", {"channel": "voice"}),
        ],
    )

    assert [item["id"] for item in created] == ["m1", "m2"]
    paths = [path for _, path, _ in calls]
    session_index = next(i for i, path in enumerate(paths) if path.endswith("/sessions"))
    message_index = next(i for i, path in enumerate(paths) if path.endswith("/messages"))
    assert session_index < message_index

    session_payload = calls[session_index][2]
    message_payload = calls[message_index][2]
    assert session_payload["id"] == encode_honcho_id("voice:neil:mac")
    assert set(session_payload["peers"]) == {"neil", "jarvis"}
    assert message_payload["messages"][0]["peer_id"] == "neil"
    assert message_payload["messages"][1]["peer_id"] == "jarvis"


def test_v3_conclusion_crud_round_trips_sidecar_metadata(tmp_path) -> None:
    conclusion_id = "con_123"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/conclusions") and request.method == "POST":
            payload = _json(request)["conclusions"][0]
            return httpx.Response(
                201,
                json=[
                    {
                        "id": conclusion_id,
                        "content": payload["content"],
                        "observer_id": payload["observer_id"],
                        "observed_id": payload["observed_id"],
                        "session_id": payload.get("session_id"),
                        "level": "explicit",
                    }
                ],
            )
        if request.url.path.endswith("/conclusions/list"):
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": conclusion_id,
                            "content": "Klaus does not work Fridays.",
                            "observer_id": "neil",
                            "observed_id": encode_honcho_id("contact:klaus"),
                            "level": "explicit",
                        }
                    ]
                },
            )
        if request.url.path.endswith(f"/conclusions/{conclusion_id}"):
            return httpx.Response(204)
        return httpx.Response(201, json={"id": _json(request).get("id", "")})

    client = HonchoV3MemoryClient(_cfg(tmp_path), transport=httpx.MockTransport(handler))
    created = client.create_conclusion(
        observed_id="contact:klaus",
        observer_id="neil",
        content="Klaus does not work Fridays.",
        metadata={"recorded_by": "neil", "observed_at": "2026-07-04", "channel": "voice"},
    )

    assert created.id == conclusion_id
    assert created.observed_id == "contact:klaus"
    assert created.metadata["recorded_by"] == "neil"

    listed = client.list_conclusions(
        observed_id="contact:klaus",
        level="explicit",
        metadata={"channel": "voice"},
    )
    assert listed == [created]

    client.delete_conclusion(conclusion_id)
    sidecar = json.loads((tmp_path / "sidecar.json").read_text(encoding="utf-8"))
    assert sidecar == {}


def test_v3_query_conclusions_filters_sidecar_metadata(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/conclusions"):
            payload = _json(request)["conclusions"][0]
            return httpx.Response(
                201,
                json=[
                    {
                        "id": "c1",
                        "content": payload["content"],
                        "observer_id": payload["observer_id"],
                        "observed_id": payload["observed_id"],
                        "level": "explicit",
                    }
                ],
            )
        if request.url.path.endswith("/conclusions/query"):
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": "c1",
                            "content": "Decision: use Honcho v3.",
                            "observer_id": "neil",
                            "observed_id": encode_honcho_id("project:jarvis"),
                            "level": "explicit",
                        }
                    ]
                },
            )
        return httpx.Response(201, json={"id": _json(request).get("id", "")})

    client = HonchoV3MemoryClient(_cfg(tmp_path), transport=httpx.MockTransport(handler))
    client.create_conclusion(
        observed_id="project:jarvis",
        observer_id="neil",
        content="Decision: use Honcho v3.",
        metadata={"project_id": "jarvis", "artifact_type": "decision"},
    )

    assert client.query_conclusions(
        "Honcho",
        observed_id="project:jarvis",
        metadata={"artifact_type": "decision"},
    )
    assert not client.query_conclusions(
        "Honcho",
        observed_id="project:jarvis",
        metadata={"artifact_type": "finding"},
    )


def test_v3_queue_status_parses_idle_state(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/queue/status")
        return httpx.Response(
            200,
            json={"pending_work_units": 2, "in_progress_work_units": 1, "queue": "messages"},
        )

    status = HonchoV3MemoryClient(_cfg(tmp_path), transport=httpx.MockTransport(handler)).queue_status()

    assert status.pending_work_units == 2
    assert status.in_progress_work_units == 1
    assert not status.idle
    assert status.raw["queue"] == "messages"


def test_v3_boundary_errors_raise_clean_httpx_errors(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    client = HonchoV3MemoryClient(_cfg(tmp_path, write_timeout_s=0.01), transport=httpx.MockTransport(handler))

    with pytest.raises(httpx.ConnectError):
        client.create_messages("voice:neil:mac", [MemoryMessage("neil", "hello")])


def test_v3_peer_cards_and_uploads_use_encoded_boundary_ids(tmp_path) -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.method == "PUT" and request.url.path.endswith("/card"):
            return httpx.Response(200, json={"peer_card": ["Name: Klaus"]})
        if request.method == "GET" and request.url.path.endswith("/card"):
            return httpx.Response(200, json={"peer_card": ["Name: Klaus"]})
        if request.url.path.endswith("/messages/upload"):
            return httpx.Response(201, json={"id": "file_1"})
        return httpx.Response(201, json={"id": _json(request).get("id", "")})

    upload = tmp_path / "spec.txt"
    upload.write_text("hello", encoding="utf-8")
    client = HonchoV3MemoryClient(_cfg(tmp_path), transport=httpx.MockTransport(handler))

    assert client.set_peer_card("contact:klaus", ["Name: Klaus"]) == ("Name: Klaus",)
    assert client.get_peer_card("contact:klaus") == ("Name: Klaus",)
    assert client.upload_file(
        "project:jarvis:uploads:spec",
        peer_id="project:jarvis",
        path=upload,
        metadata={"artifact_type": "spec"},
    ) == {"id": "file_1"}

    assert any(encode_honcho_id("contact:klaus") in path for path in paths)
    assert any(path.endswith("/messages/upload") for path in paths)


def test_v3_explicit_create_session_accepts_membership(tmp_path) -> None:
    payloads: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payloads.append(_json(request))
        return httpx.Response(201, json={"id": _json(request).get("id", "")})

    client = HonchoV3MemoryClient(_cfg(tmp_path), transport=httpx.MockTransport(handler))
    session = client.create_session(
        "project:jarvis:inbox",
        peers=[SessionPeer("project:jarvis", observe_me=True, observe_others=False)],
    )

    assert session.id == "project:jarvis:inbox"
    session_payload = next(payload for payload in payloads if payload.get("peers"))
    assert session_payload["id"] == encode_honcho_id("project:jarvis:inbox")
    assert encode_honcho_id("project:jarvis") in session_payload["peers"]
