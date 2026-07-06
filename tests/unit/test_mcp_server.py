from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import httpx
import pytest

from jarvis.brain.capabilities import RequestContext
from jarvis.brain.memory_client import RepresentationRecord
from jarvis.brain.memory_outbox import CurationOutbox
from jarvis.brain.project_management import ProjectOperationError
from jarvis.brain.registry import ProjectEntry
from jarvis.config import Config
from jarvis.mcp_server.adapters import (
    MCPAccessError,
    MCPCockpitConnector,
    MCP_SEND_TURN_CAPABILITIES,
    JarvisMCPService,
    mcp_send_turn_context,
)
from jarvis.mcp_server.server import MCPServerRuntime, _BearerAuthASGI, build_mcp
from jarvis.mcp_server.tokens import MCPTokenStore


class Response:
    def __init__(self, data: dict[str, Any], status_code: int = 200) -> None:
        self._data = data
        self.status_code = status_code
        self.text = json.dumps(data)

    def json(self) -> dict[str, Any]:
        return self._data

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(self.text)


class FakeMemory:
    def __init__(self) -> None:
        self.representations: dict[str, str] = {}
        self.reads: list[dict[str, Any]] = []
        self.chats: list[dict[str, Any]] = []
        self.sessions: list[dict[str, Any]] = []
        self.messages: list[dict[str, Any]] = []

    def read_cached_representation(self, user: str | None = None) -> str:
        return self.representations.get(user or "", "cached")

    def read_representation(self, peer_id: str, **kwargs: Any) -> RepresentationRecord:
        self.reads.append({"peer_id": peer_id, **kwargs})
        return RepresentationRecord(
            peer_id=peer_id,
            representation=self.representations.get(peer_id, "live representation"),
        )

    def dialectic_chat(self, peer_id: str, query: str, **kwargs: Any) -> str:
        self.chats.append({"peer_id": peer_id, "query": query, **kwargs})
        return f"answer for {peer_id}: {query}"

    def create_session(self, session_id: str, **kwargs: Any) -> dict[str, Any]:
        row = {"session_id": session_id, **kwargs}
        self.sessions.append(row)
        return row

    def create_messages(self, session_id: str, messages: list[Any]) -> list[dict[str, Any]]:
        rows = [
            {
                "session_id": session_id,
                "peer_id": message.peer_id,
                "content": message.content,
                "metadata": dict(message.metadata),
            }
            for message in messages
        ]
        self.messages.extend(rows)
        return rows


class _Fn:
    def __init__(self, name: str, arguments: str) -> None:
        self.name = name
        self.arguments = arguments


class _Msg:
    def __init__(self, content: str = "") -> None:
        self.content = content
        self.tool_calls = []


class FakeGateway:
    def __init__(self, reply: str = "MCP reply.") -> None:
        self.reply = reply
        self.tools: list[list[dict[str, Any]] | None] = []

    async def complete_with_tools(
        self,
        messages: list[dict[str, Any]],
        *,
        model: str | None = None,
        tools: list[dict[str, Any]] | None = None,
        usage_out: dict[str, Any] | None = None,
    ) -> _Msg:
        _ = (messages, model, usage_out)
        self.tools.append(tools)
        return _Msg(self.reply)


class FakeProjectClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def execute(self, ctx, op: str, payload: dict[str, Any]) -> dict[str, Any]:  # noqa: ANN001
        self.calls.append({"identity": ctx.identity, "op": op, "payload": dict(payload)})
        if op == "project.file.upload":
            return {
                "project_id": payload["project_id"],
                "doc_id": "spec-123",
                "session_id": "project:jarvis:uploads:spec-123",
                "original_path": "/tmp/spec.md",
                "metadata": {"channel": payload.get("channel")},
                "ingestion": {"queued": True},
            }
        if op == "project.file.retract":
            return {"project_id": payload["project_id"], "doc_id": payload["doc_id"], "retracted": True}
        if op == "project.file.list":
            return {"project_id": payload["project_id"], "files": [{"doc_id": "spec-123"}]}
        if op in {"project.memory.forget", "project.memory.correct"}:
            if ctx.identity not in {"neil", "viewer"}:
                raise ProjectOperationError("not_found", "project not found", status=404)
            return {"project_id": payload["project_id"], "result": "Forgotten." if op.endswith("forget") else "Corrected."}
        return {"project": {"id": payload.get("project_id") or payload.get("id"), "name": payload.get("name", "Project")}}


def _cfg(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    auth_mode: str = "hybrid",
    resource_url: str = "",
    oauth_issuer: str = "",
    oauth_jwks_url: str = "",
    oauth_required_scopes: str = "",
) -> Config:
    tmp_path.mkdir(parents=True, exist_ok=True)
    env = tmp_path / ".env"
    env.write_text(
        "\n".join(
            [
                f"CAPS_USERS_DIR={tmp_path / 'users'}",
                "CAPS_DEFAULT_CAPABILITIES=",
                f"ORCHESTRATION_WORKSPACE={tmp_path / 'orchestration'}",
                f"REGISTRY_PATH={tmp_path / 'registry.json'}",
                "MEMORY_BACKEND=v3",
                f"MEMORY_CACHE_PATH={tmp_path / 'cache.json'}",
                f"MEMORY_CURATION_OUTBOX_PATH={tmp_path / 'outbox.jsonl'}",
                f"MCP_SERVE_TOKEN_STORE_PATH={tmp_path / 'tokens.json'}",
                f"MCP_SERVE_AUTH_MODE={auth_mode}",
                f"MCP_SERVE_RESOURCE_URL={resource_url}",
                f"MCP_SERVE_OAUTH_ISSUER={oauth_issuer}",
                f"MCP_SERVE_OAUTH_JWKS_URL={oauth_jwks_url}",
                f"MCP_SERVE_OAUTH_REQUIRED_SCOPES={oauth_required_scopes}",
                "MCP_SERVE_OAUTH_JWKS_TTL_S=300",
                "MCP_SERVE_OAUTH_JWKS_MIN_REFRESH_S=30",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("JARVIS_ENV_FILE", str(env))
    cfg = Config()
    _seed_users(cfg)
    _seed_registry(cfg)
    return cfg


def _seed_users(cfg: Config) -> None:
    users = Path(cfg.capabilities.users_dir)
    users.mkdir(parents=True, exist_ok=True)
    users.joinpath("neil.md").write_text(
        "---\nscope: personal\nhoncho_peer: neil\noauth_subjects: [ba_neil]\ncapabilities: [memory.query, memory.curate, project.switch]\n---\n# Neil\n",
        encoding="utf-8",
    )
    users.joinpath("viewer.md").write_text(
        "---\nscope: personal\nhoncho_peer: viewer\ncapabilities: [memory.query]\n---\n# Viewer\n",
        encoding="utf-8",
    )
    users.joinpath("guest.md").write_text(
        "---\nscope: personal\nhoncho_peer: guest\ncapabilities: []\n---\n# Guest\n",
        encoding="utf-8",
    )
    users.joinpath("curator.md").write_text(
        "---\nscope: personal\nhoncho_peer: curator\ncapabilities: [memory.query, memory.curate]\n---\n# Curator\n",
        encoding="utf-8",
    )


def _seed_registry(cfg: Config) -> None:
    path = Path(cfg.registry.path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "projects": [
                    {
                        "id": "jarvis",
                        "name": "Jarvis",
                        "aliases": ["the jarvis project"],
                        "owner": "neil",
                        "members": ["neil", "viewer"],
                        "visibility": "shared",
                        "status": "active",
                        "repos": [],
                        "links": {"jira": "", "urls": []},
                        "files_root": "",
                    },
                    {
                        "id": "private",
                        "name": "Private",
                        "owner": "alice",
                        "members": ["alice"],
                        "visibility": "private",
                        "status": "active",
                        "repos": [],
                        "links": {"jira": "", "urls": []},
                        "files_root": "",
                    },
                ],
                "contacts": [],
            }
        ),
        encoding="utf-8",
    )


def _oauth_cfg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, scopes: str = "mcp:use") -> Config:
    return _cfg(
        tmp_path,
        monkeypatch,
        auth_mode="hybrid",
        resource_url="http://localhost:8795",
        oauth_issuer="https://cockpit.example",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes=scopes,
    )


def _oauth_fixture(*, kid: str = "mcp-test-key") -> tuple[dict[str, Any], Callable[..., Response]]:
    jwt = pytest.importorskip("jwt")
    cryptography_rsa = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.rsa")
    cryptography_serialization = pytest.importorskip("cryptography.hazmat.primitives.serialization")

    private_key = cryptography_rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private_key.private_bytes(
        encoding=cryptography_serialization.Encoding.PEM,
        format=cryptography_serialization.PrivateFormat.PKCS8,
        encryption_algorithm=cryptography_serialization.NoEncryption(),
    )
    public_jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(private_key.public_key()))
    public_jwk.update({"kid": kid, "use": "sig", "alg": "RS256"})
    jwks = {"keys": [public_jwk]}
    calls = {"jwks": 0}

    def sign(
        *,
        issuer: str = "https://cockpit.example",
        audience: str | list[str] = "http://localhost:8795",
        subject: str = "ba_neil",
        scope: str = "mcp:use",
        expires_delta: timedelta = timedelta(minutes=5),
        token_kid: str = kid,
        signing_key: Any = private_pem,
    ) -> str:
        now = datetime.now(UTC)
        return jwt.encode(
            {
                "iss": issuer,
                "sub": subject,
                "aud": audience,
                "scope": scope,
                "exp": now + expires_delta,
                "iat": now,
            },
            signing_key,
            algorithm="RS256",
            headers={"kid": token_kid},
        )

    def jwks_get(url: str, **_kwargs: Any) -> Response:
        if url == "https://cockpit.example/api/auth/jwks":
            calls["jwks"] += 1
            return Response(jwks)
        return Response({})

    return {"sign": sign, "calls": calls, "jwks": jwks}, jwks_get


def _bad_signature_token(fixture: dict[str, Any]) -> str:
    other, _get = _oauth_fixture(kid="mcp-test-key")
    return other["sign"]()


async def _asgi_request(app: Any, path: str, *, token: str = "") -> dict[str, Any]:
    sent: list[dict[str, Any]] = []
    headers = [(b"authorization", f"Bearer {token}".encode("ascii"))] if token else []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await app(
        {"type": "http", "method": "GET", "path": path, "headers": headers},
        receive,
        send,
    )
    start = next(item for item in sent if item["type"] == "http.response.start")
    body = b"".join(item.get("body", b"") for item in sent if item["type"] == "http.response.body")
    return {
        "status": start["status"],
        "headers": {name.decode("latin1"): value.decode("latin1") for name, value in start["headers"]},
        "body": body.decode("utf-8"),
    }


def _inner_app(runtime: MCPServerRuntime, seen: list[RequestContext] | None = None):  # noqa: ANN202
    async def inner(scope, receive, send):  # noqa: ANN001
        _ = (scope, receive)
        ctx = runtime.requester()
        if seen is not None:
            seen.append(ctx)
        body = json.dumps({"identity": ctx.identity, "capabilities": sorted(ctx.capabilities)}).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    return inner


def test_token_add_list_revoke_roundtrip(tmp_path) -> None:
    store = MCPTokenStore(tmp_path / "tokens.json")
    token, record = store.add(principal="neil", name="Claude Code")

    assert store.resolve(token).principal == "neil"
    listed = store.list()
    assert [(item.token_id, item.principal, item.name) for item in listed] == [
        (record.token_id, "neil", "Claude Code")
    ]
    assert token not in (tmp_path / "tokens.json").read_text(encoding="utf-8")

    revoked = store.revoke(record.token_id[:12])
    assert revoked.revoked
    assert store.resolve(token) is None
    assert store.list() == []
    assert store.list(include_revoked=True)[0].revoked


def test_token_principal_context_inherits_user_capabilities(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    token, _record = MCPTokenStore(cfg.mcp_serve.token_store_path).add(principal="neil")
    resolved = MCPTokenStore(cfg.mcp_serve.token_store_path).resolve(token)

    ctx = JarvisMCPService(cfg, memory=FakeMemory()).context_for_principal(resolved.principal)

    assert ctx.identity == "neil"
    assert ctx.memory_peer == "neil"
    assert ctx.channel == "mcp"
    assert {"memory.query", "memory.curate", "project.switch"} <= set(ctx.capabilities)


def test_oauth_discovery_and_challenge_header(tmp_path, monkeypatch) -> None:
    fixture, jwks_get = _oauth_fixture()
    cfg = _oauth_cfg(tmp_path, monkeypatch)
    service = JarvisMCPService(cfg, memory=FakeMemory())
    runtime = MCPServerRuntime(service)
    app = _BearerAuthASGI(_inner_app(runtime), service, cfg, http_get=jwks_get)

    metadata = asyncio.run(_asgi_request(app, "/.well-known/oauth-protected-resource"))
    missing = asyncio.run(_asgi_request(app, "/mcp"))
    invalid = asyncio.run(_asgi_request(app, "/mcp", token="not-a-jwt"))
    expired = asyncio.run(
        _asgi_request(
            app,
            "/mcp",
            token=fixture["sign"](expires_delta=timedelta(minutes=-2)),
        )
    )

    assert metadata["status"] == 200
    assert json.loads(metadata["body"]) == {
        "resource": "http://localhost:8795",
        "authorization_servers": ["https://cockpit.example"],
        "bearer_methods_supported": ["header"],
        "resource_name": "Jarvis MCP",
        "scopes_supported": ["mcp:use"],
    }
    challenge = 'Bearer resource_metadata="http://localhost:8795/.well-known/oauth-protected-resource"'
    assert missing["status"] == 401
    assert missing["headers"]["www-authenticate"] == challenge
    assert invalid["status"] == 401
    assert invalid["headers"]["www-authenticate"] == challenge
    assert expired["status"] == 401
    assert expired["headers"]["www-authenticate"] == challenge


def test_oauth_discovery_404s_in_legacy_without_issuer(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch, auth_mode="legacy")
    service = JarvisMCPService(cfg, memory=FakeMemory())
    app = _BearerAuthASGI(_inner_app(MCPServerRuntime(service)), service, cfg)

    response = asyncio.run(_asgi_request(app, "/.well-known/oauth-protected-resource"))

    assert response["status"] == 404


def test_oauth_validation_matrix_rejects_bad_jwts(tmp_path, monkeypatch) -> None:
    fixture, jwks_get = _oauth_fixture()
    cfg = _oauth_cfg(tmp_path, monkeypatch)
    service = JarvisMCPService(cfg, memory=FakeMemory())
    runtime = MCPServerRuntime(service)
    app = _BearerAuthASGI(_inner_app(runtime), service, cfg, http_get=jwks_get)

    valid = asyncio.run(_asgi_request(app, "/mcp", token=fixture["sign"]()))
    invalid_tokens = [
        fixture["sign"](issuer="https://evil.example"),
        fixture["sign"](audience="jarvis-cockpit-api"),
        fixture["sign"](scope="other:scope"),
        fixture["sign"](expires_delta=timedelta(minutes=-2)),
        _bad_signature_token(fixture),
    ]
    rejected = [asyncio.run(_asgi_request(app, "/mcp", token=token)) for token in invalid_tokens]

    assert valid["status"] == 200
    assert json.loads(valid["body"])["identity"] == "neil"
    assert [response["status"] for response in rejected] == [401, 401, 401, 401, 401]


def test_oauth_principal_mapping_and_capability_parity(tmp_path, monkeypatch) -> None:
    fixture, jwks_get = _oauth_fixture()
    cfg = _oauth_cfg(tmp_path, monkeypatch)
    service = JarvisMCPService(cfg, memory=FakeMemory())
    runtime = MCPServerRuntime(service)
    seen: list[RequestContext] = []
    app = _BearerAuthASGI(_inner_app(runtime, seen), service, cfg, http_get=jwks_get)

    mapped = asyncio.run(_asgi_request(app, "/mcp", token=fixture["sign"](subject="ba_neil")))
    fallback = asyncio.run(_asgi_request(app, "/mcp", token=fixture["sign"](subject="viewer")))
    unknown = asyncio.run(_asgi_request(app, "/mcp", token=fixture["sign"](subject="missing-sub")))
    static_token, _record = MCPTokenStore(cfg.mcp_serve.token_store_path).add(principal="neil")
    static = asyncio.run(_asgi_request(app, "/mcp", token=static_token))

    users = Path(cfg.capabilities.users_dir)
    users.joinpath("viewer.md").write_text(
        "---\nscope: personal\nhoncho_peer: viewer\noauth_subjects: [ba_neil]\ncapabilities: [memory.query]\n---\n# Viewer\n",
        encoding="utf-8",
    )
    duplicate = asyncio.run(_asgi_request(app, "/mcp", token=fixture["sign"](subject="ba_neil")))

    assert mapped["status"] == 200
    assert fallback["status"] == 200
    assert json.loads(fallback["body"])["identity"] == "viewer"
    assert unknown["status"] == 401
    assert static["status"] == 200
    assert duplicate["status"] == 401
    oauth_neil = next(ctx for ctx in seen if ctx.identity == "neil")
    static_neil = service.context_for_principal("neil")
    assert oauth_neil.capabilities == static_neil.capabilities
    assert oauth_neil.memory_peer == static_neil.memory_peer
    assert oauth_neil.channel == static_neil.channel == "mcp"


def test_hybrid_ordering_degradation_and_jwks_fail_closed(tmp_path, monkeypatch, caplog) -> None:
    fixture, jwks_get = _oauth_fixture()
    cfg = _oauth_cfg(tmp_path, monkeypatch)
    service = JarvisMCPService(cfg, memory=FakeMemory())
    runtime = MCPServerRuntime(service)
    static_token, _record = MCPTokenStore(cfg.mcp_serve.token_store_path).add(principal="viewer")
    app = _BearerAuthASGI(_inner_app(runtime), service, cfg, http_get=jwks_get)

    static = asyncio.run(_asgi_request(app, "/mcp", token=static_token))
    assert static["status"] == 200
    assert json.loads(static["body"])["identity"] == "viewer"
    assert fixture["calls"]["jwks"] == 0

    jwt_response = asyncio.run(_asgi_request(app, "/mcp", token=fixture["sign"]()))
    garbage = asyncio.run(_asgi_request(app, "/mcp", token="garbage"))

    assert jwt_response["status"] == 200
    assert fixture["calls"]["jwks"] == 1
    assert garbage["status"] == 401

    partial_cfg = _cfg(
        tmp_path / "partial",
        monkeypatch,
        auth_mode="hybrid",
        resource_url="http://localhost:8795",
        oauth_issuer="https://cockpit.example",
    )
    partial_service = JarvisMCPService(partial_cfg, memory=FakeMemory())
    partial_token, _ = MCPTokenStore(partial_cfg.mcp_serve.token_store_path).add(principal="neil")
    caplog.clear()
    partial_app = _BearerAuthASGI(_inner_app(MCPServerRuntime(partial_service)), partial_service, partial_cfg, http_get=jwks_get)
    partial_static = asyncio.run(_asgi_request(partial_app, "/mcp", token=partial_token))
    partial_jwt = asyncio.run(_asgi_request(partial_app, "/mcp", token=fixture["sign"]()))

    assert partial_static["status"] == 200
    assert partial_jwt["status"] == 401
    assert "OAuth disabled" in caplog.text

    def failing_jwks_get(_url: str, **_kwargs: Any) -> Response:
        raise RuntimeError("jwks down")

    fail_cfg = _oauth_cfg(tmp_path / "fail", monkeypatch)
    fail_service = JarvisMCPService(fail_cfg, memory=FakeMemory())
    fail_static_token, _ = MCPTokenStore(fail_cfg.mcp_serve.token_store_path).add(principal="neil")
    fail_app = _BearerAuthASGI(_inner_app(MCPServerRuntime(fail_service)), fail_service, fail_cfg, http_get=failing_jwks_get)

    assert asyncio.run(_asgi_request(fail_app, "/mcp", token=fixture["sign"]()))["status"] == 401
    assert asyncio.run(_asgi_request(fail_app, "/mcp", token=fail_static_token))["status"] == 200


def test_unknown_principal_is_denied_by_default(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    service = JarvisMCPService(cfg, memory=FakeMemory())

    with pytest.raises(MCPAccessError):
        service.context_for_principal("missing")


def test_project_list_is_membership_filtered(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    service = JarvisMCPService(cfg, memory=FakeMemory())
    ctx = service.context_for_principal("viewer")

    body = asyncio.run(service.project_list(ctx))

    assert [project["id"] for project in body["projects"]] == ["jarvis"]


def test_project_get_is_membership_filtered(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    service = JarvisMCPService(cfg, memory=FakeMemory())
    ctx = service.context_for_principal("viewer")

    body = asyncio.run(service.project_get(ctx, project_id="jarvis"))
    assert body["project"]["name"] == "Jarvis"

    with pytest.raises(MCPAccessError):
        asyncio.run(service.project_get(ctx, project_id="private"))


def test_memory_search_is_capability_and_access_gated(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    memory = FakeMemory()
    service = JarvisMCPService(cfg, memory=memory)
    viewer = service.context_for_principal("viewer")

    own = asyncio.run(service.memory_search(viewer, search_query="what do you know?"))
    assert own["result"] == "answer for viewer: what do you know?"

    with pytest.raises(MCPAccessError, match="project is not visible"):
        asyncio.run(service.memory_search(viewer, search_query="secret", target="project:private"))

    guest = service.context_for_principal("guest")
    with pytest.raises(MCPAccessError, match="capability"):
        asyncio.run(service.memory_search(guest, search_query="anything"))


def test_record_finding_carries_mcp_audit_metadata(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    service = JarvisMCPService(cfg, memory=FakeMemory())
    ctx = service.context_for_principal("neil")

    body = asyncio.run(
        service.record_finding(
            ctx,
            project="Jarvis",
            content="The cache read stays local.",
            agent="claude-code",
            observed_at="2026-07-05",
        )
    )

    assert body["result"].startswith("Noted")
    entry = CurationOutbox(cfg.memory.curation_outbox_path).pending_entries()[0]
    assert entry.observed_id == "project:jarvis"
    assert entry.content == "The cache read stays local."
    assert entry.metadata["recorded_by"] == "neil"
    assert entry.metadata["channel"] == "mcp"
    assert entry.metadata["source"] == "mcp"
    assert entry.metadata["agent"] == "claude-code"
    assert entry.metadata["project_id"] == "jarvis"
    assert entry.metadata["artifact_type"] == "finding"
    assert entry.metadata["observed_at"] == "2026-07-05"


def test_upload_file_relays_to_brain_without_base64_tool_arg(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    project_client = FakeProjectClient()
    service = JarvisMCPService(cfg, memory=FakeMemory(), project_client=project_client)
    ctx = service.context_for_principal("neil")

    body = asyncio.run(
        service.upload_file(
            ctx,
            project_id="jarvis",
            content="# Spec",
            filename="spec.md",
            agent="claude-code",
        )
    )

    assert body["doc_id"] == "spec-123"
    assert project_client.calls == [
        {
            "identity": "neil",
            "op": "project.file.upload",
            "payload": {
                "project_id": "jarvis",
                "artifact_type": "spec",
                "title": "",
                "agent": "claude-code",
                "channel": "mcp",
                "content_text": "# Spec",
                "filename": "spec.md",
            },
        }
    ]


def test_mcp_file_list_and_memory_curation_relay_to_brain(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    project_client = FakeProjectClient()
    service = JarvisMCPService(cfg, memory=FakeMemory(), project_client=project_client)
    ctx = service.context_for_principal("neil")

    files = asyncio.run(service.project_list_files(ctx, project_id="jarvis"))
    forgotten = asyncio.run(service.forget(ctx, project_id="jarvis", query="old fact", confirm=True, conclusion_ids=["c1"]))
    corrected = asyncio.run(
        service.correct(
            ctx,
            project_id="jarvis",
            query="wrong fact",
            replacement="right fact",
            confirm=True,
            conclusion_ids=["c2"],
        )
    )

    assert files["files"] == [{"doc_id": "spec-123"}]
    assert forgotten == {"result": "Forgotten."}
    assert corrected == {"result": "Corrected."}
    assert [call["op"] for call in project_client.calls] == [
        "project.file.list",
        "project.memory.forget",
        "project.memory.correct",
    ]
    assert project_client.calls[1]["payload"]["channel"] == "mcp"
    assert project_client.calls[2]["payload"]["source"] == "mcp"


def test_mcp_forget_correct_non_member_project_memory_is_denied_by_brain_gate(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    service = JarvisMCPService(cfg, memory=FakeMemory(), project_client=FakeProjectClient())
    curator = service.context_for_principal("curator")

    with pytest.raises(MCPAccessError, match="project not found"):
        asyncio.run(service.forget(curator, project_id="jarvis", query="old fact"))
    with pytest.raises(MCPAccessError, match="project not found"):
        asyncio.run(service.correct(curator, project_id="jarvis", query="old fact", replacement="new fact"))


def test_mcp_project_writes_relay_to_brain(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    project_client = FakeProjectClient()
    service = JarvisMCPService(cfg, memory=FakeMemory(), project_client=project_client)
    ctx = service.context_for_principal("neil")

    asyncio.run(service.project_create(ctx, id="new-project", name="New Project"))
    asyncio.run(service.project_update(ctx, project_id="jarvis", name="Renamed"))
    asyncio.run(service.project_set_visibility(ctx, project_id="jarvis", visibility="private"))
    asyncio.run(service.project_set_members(ctx, project_id="jarvis", members=["neil", "viewer"]))
    asyncio.run(service.project_archive(ctx, project_id="jarvis", archived=True))
    asyncio.run(service.project_delete(ctx, project_id="jarvis"))

    assert [call["op"] for call in project_client.calls] == [
        "project.create",
        "project.update",
        "project.visibility.set",
        "project.members.set",
        "project.archive",
        "project.delete",
    ]


def test_send_turn_context_caps_host_device_tool_ceiling(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    cfg.capabilities.default_capabilities = (
        "memory.query,memory.curate,project.switch,web.search,files.read,files.write,"
        "worker.code,worker.shell,worker.browser,background.run"
    )
    memory = FakeMemory()
    service = JarvisMCPService(cfg, memory=memory)
    full_ctx = service.context_for_principal("neil")
    assert "worker.code" in full_ctx.capabilities
    assert "web.search" in full_ctx.capabilities

    project = service.registry.get_project("jarvis")
    connector = MCPCockpitConnector(cfg, memory=memory, gateway=FakeGateway())

    session = connector._make_session(  # noqa: SLF001 - security regression probes the built registry.
        mcp_send_turn_context(full_ctx),
        project=project,
        memory=memory,
    )
    offered = {tool.name for tool in session._registry.available_for(session._ctx)}  # noqa: SLF001

    assert session._ctx.capabilities == MCP_SEND_TURN_CAPABILITIES  # noqa: SLF001
    assert {"memory_search", "add_finding", "record_decision", "switch_project"} <= offered
    assert {
        "web_search",
        "fetch_page",
        "read_file",
        "write_file",
        "start_coding_job",
        "run_shell",
        "browser_open",
        "run_in_background",
    }.isdisjoint(offered)


def test_send_turn_persists_messages_with_mcp_channel(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    memory = FakeMemory()
    gateway = FakeGateway("done")
    connector = MCPCockpitConnector(cfg, memory=memory, gateway=gateway)
    service = JarvisMCPService(cfg, memory=memory, cockpit=connector)
    ctx = service.context_for_principal("neil")
    project = service.registry.get_project("jarvis")
    thread = asyncio.run(connector.open_thread(project, ctx, title="MCP thread"))

    body = asyncio.run(
        service.send_turn(
            ctx,
            project_id="jarvis",
            thread_id=thread.thread_id,
            text="summarise status",
        )
    )

    assert body["reply"] == "done"
    persisted = [message for message in memory.messages if message["session_id"] == thread.session_id]
    assert [message["metadata"]["channel"] for message in persisted] == ["mcp", "mcp"]
    assert [message["metadata"]["role"] for message in persisted] == ["user", "assistant"]


def test_mcp_archive_unarchive_thread_roundtrip(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    memory = FakeMemory()
    connector = MCPCockpitConnector(cfg, memory=memory, gateway=FakeGateway())
    service = JarvisMCPService(cfg, memory=memory, cockpit=connector)
    ctx = service.context_for_principal("neil")
    project = service.registry.get_project("jarvis")
    thread = asyncio.run(connector.open_thread(project, ctx, title="MCP archive"))

    archived = asyncio.run(
        service.archive_thread(
            ctx,
            project_id="jarvis",
            thread_id=thread.thread_id,
            reason="done",
        )
    )
    with pytest.raises(MCPAccessError, match="thread is archived"):
        asyncio.run(service.send_turn(ctx, project_id="jarvis", thread_id=thread.thread_id, text="continue"))
    unarchived = asyncio.run(service.unarchive_thread(ctx, project_id="jarvis", thread_id=thread.thread_id))

    assert archived["thread"]["archived_at"]
    assert archived["thread"]["archived_by"] == "neil"
    assert archived["thread"]["archive_reason"] == "done"
    assert unarchived["thread"]["archived_at"] == ""
    assert unarchived["thread"]["archived_by"] == ""
    assert unarchived["thread"]["archive_reason"] == ""


def test_mcp_archive_thread_requires_project_membership(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    service = JarvisMCPService(cfg, memory=FakeMemory())
    service.registry.create_project(
        ProjectEntry(
            id="household-thread",
            name="Household Thread",
            owner="neil",
            members=("neil",),
            visibility="household",
        )
    )
    ctx = service.context_for_principal("viewer")
    visible = asyncio.run(service.project_get(ctx, project_id="household-thread"))
    assert visible["project"]["id"] == "household-thread"

    with pytest.raises(MCPAccessError, match="not editable"):
        asyncio.run(
            service.archive_thread(
                ctx,
                project_id="household-thread",
                thread_id="thread_any",
                reason="done",
            )
        )
    with pytest.raises(MCPAccessError, match="not editable"):
        asyncio.run(service.unarchive_thread(ctx, project_id="household-thread", thread_id="thread_any"))


def test_mcp_archive_thread_requires_visible_project(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    service = JarvisMCPService(cfg, memory=FakeMemory())
    ctx = service.context_for_principal("viewer")

    with pytest.raises(MCPAccessError, match="project not found"):
        asyncio.run(
            service.archive_thread(
                ctx,
                project_id="private",
                thread_id="thread_missing",
                reason="done",
            )
        )


def test_mcp_request_context_is_set_and_cleared_per_request(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    neil_token, _ = MCPTokenStore(cfg.mcp_serve.token_store_path).add(principal="neil")
    viewer_token, _ = MCPTokenStore(cfg.mcp_serve.token_store_path).add(principal="viewer")
    service = JarvisMCPService(cfg, memory=FakeMemory())
    runtime = MCPServerRuntime(service)
    seen: list[str] = []

    async def inner(scope, receive, send):  # noqa: ANN001
        _ = (receive, send)
        seen.append(runtime.requester().identity)

    app = _BearerAuthASGI(inner, service, cfg)

    async def call(token: str) -> None:
        sent: list[dict[str, Any]] = []
        await app(
            {
                "type": "http",
                "headers": [(b"authorization", f"Bearer {token}".encode("ascii"))],
            },
            lambda: None,
            sent.append,
        )

    async def go() -> None:
        await asyncio.gather(call(neil_token), call(viewer_token))

    asyncio.run(go())

    assert sorted(seen) == ["neil", "viewer"]
    with pytest.raises(MCPAccessError):
        runtime.requester()


def test_oauth_real_mcp_app_serves_discovery_challenge_and_tool_call(tmp_path, monkeypatch) -> None:
    pytest.importorskip("mcp")
    uvicorn = pytest.importorskip("uvicorn")
    from jarvis.mcp.client import MCPClient

    fixture, jwks_get = _oauth_fixture()
    cfg = _oauth_cfg(tmp_path, monkeypatch)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    cfg.mcp_serve.host = "127.0.0.1"
    cfg.mcp_serve.port = port
    cfg.mcp_serve.resource_url = f"http://127.0.0.1:{port}"
    service = JarvisMCPService(cfg, memory=FakeMemory())
    runtime = MCPServerRuntime(service)
    mcp = build_mcp(runtime)
    app = _BearerAuthASGI(mcp.streamable_http_app(), service, cfg, http_get=jwks_get)
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    )
    thread = threading.Thread(target=lambda: asyncio.run(server.serve()), daemon=True)
    thread.start()
    started_at = time.monotonic()
    while not server.started and time.monotonic() - started_at < 5:
        time.sleep(0.05)
    assert server.started

    token = fixture["sign"](audience=cfg.mcp_serve.resolved_resource_url)
    try:
        metadata = httpx.get(f"{cfg.mcp_serve.resolved_resource_url}/.well-known/oauth-protected-resource")
        challenge = httpx.get(f"{cfg.mcp_serve.resolved_resource_url}/mcp")
        assert metadata.status_code == 200
        assert metadata.json()["resource"] == cfg.mcp_serve.resolved_resource_url
        assert challenge.status_code == 401
        assert challenge.headers["www-authenticate"] == (
            f'Bearer resource_metadata="{cfg.mcp_serve.resolved_resource_url}/.well-known/oauth-protected-resource"'
        )

        async def call_tool() -> str:
            from jarvis.config import MCPServerSpec

            client = MCPClient(
                MCPServerSpec(
                    name="jarvis",
                    transport="http",
                    url=f"{cfg.mcp_serve.resolved_resource_url}/mcp",
                    headers={"Authorization": f"Bearer {token}"},
                ),
                call_timeout_s=10.0,
            )
            await asyncio.wait_for(client.connect(), 20.0)
            try:
                return await client.call("project_list", {"include_archived": False})
            finally:
                await client.aclose()

        out = asyncio.run(call_tool())
        assert "Jarvis" in out
    finally:
        server.should_exit = True
        thread.join(timeout=5)


def test_service_does_not_construct_memory_client_for_registry_reads(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)

    def fail_memory_client(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("MCP server should not construct memory for project_list")

    monkeypatch.setattr("jarvis.brain.memory_client.MemoryClient", fail_memory_client)
    service = JarvisMCPService(cfg)
    ctx = service.context_for_principal("viewer")

    body = asyncio.run(service.project_list(ctx))

    assert [project["id"] for project in body["projects"]] == ["jarvis"]
