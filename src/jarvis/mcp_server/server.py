"""FastMCP transport wrapper for Jarvis as an MCP server."""

from __future__ import annotations

import contextvars
import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import httpx

from jarvis.brain.capabilities import RequestContext
from jarvis.config import Config
from jarvis.mcp_server.adapters import JarvisMCPService, MCPAccessError
from jarvis.mcp_server.tokens import MCPTokenStore
from jarvis.oauth import OAuthTokenValidator, OAuthValidationError, auth_mode, required_scopes
from jarvis.users import User, load_users

_REQUEST_CONTEXT: contextvars.ContextVar[RequestContext | None] = contextvars.ContextVar(
    "jarvis_mcp_request_context",
    default=None,
)
logger = logging.getLogger(__name__)
_METADATA_PATH = "/.well-known/oauth-protected-resource"


@dataclass(frozen=True)
class MCPServerRuntime:
    service: JarvisMCPService
    fixed_context: RequestContext | None = None

    def requester(self) -> RequestContext:
        ctx = self.fixed_context or _REQUEST_CONTEXT.get()
        if ctx is None:
            raise MCPAccessError("missing MCP principal")
        return ctx


def build_mcp(runtime: MCPServerRuntime):  # noqa: ANN201
    """Build a FastMCP app. Imports the optional SDK only on serve."""
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP(
        "Jarvis",
        instructions=(
            "Use Jarvis household brain tools through the authenticated principal. "
            "All project and memory access is capability-gated."
        ),
        host=runtime.service.cfg.mcp_serve.host,
        port=runtime.service.cfg.mcp_serve.port,
        streamable_http_path="/mcp",
        stateless_http=True,
    )

    @mcp.tool(name="project_list")
    async def project_list(include_archived: bool = False) -> dict[str, Any]:
        return await runtime.service.project_list(
            runtime.requester(),
            include_archived=include_archived,
        )

    @mcp.tool(name="project_get")
    async def project_get(project_id: str) -> dict[str, Any]:
        return await runtime.service.project_get(runtime.requester(), project_id=project_id)

    @mcp.tool(name="project_create")
    async def project_create(
        id: str,
        name: str,
        aliases: list[str] | None = None,
        visibility: str = "household",
        status: str = "active",
        files_root: str = "",
        repos: list[dict[str, Any]] | None = None,
        links: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await runtime.service.project_create(
            runtime.requester(),
            id=id,
            name=name,
            aliases=aliases or [],
            visibility=visibility,
            status=status,
            files_root=files_root,
            repos=repos or [],
            links=links or {},
        )

    @mcp.tool(name="project_update")
    async def project_update(
        project_id: str,
        name: str = "",
        aliases: list[str] | None = None,
        status: str = "",
        files_root: str = "",
        repos: list[dict[str, Any]] | None = None,
        links: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        fields: dict[str, Any] = {}
        if name:
            fields["name"] = name
        if aliases is not None:
            fields["aliases"] = aliases
        if status:
            fields["status"] = status
        if files_root:
            fields["files_root"] = files_root
        if repos is not None:
            fields["repos"] = repos
        if links is not None:
            fields["links"] = links
        return await runtime.service.project_update(runtime.requester(), project_id=project_id, **fields)

    @mcp.tool(name="project_set_repos")
    async def project_set_repos(project_id: str, repos: list[dict[str, Any]]) -> dict[str, Any]:
        return await runtime.service.project_set_repos(runtime.requester(), project_id=project_id, repos=repos)

    @mcp.tool(name="project_set_visibility")
    async def project_set_visibility(project_id: str, visibility: str) -> dict[str, Any]:
        return await runtime.service.project_set_visibility(
            runtime.requester(),
            project_id=project_id,
            visibility=visibility,
        )

    @mcp.tool(name="project_set_members")
    async def project_set_members(project_id: str, members: list[str]) -> dict[str, Any]:
        return await runtime.service.project_set_members(runtime.requester(), project_id=project_id, members=members)

    @mcp.tool(name="project_archive")
    async def project_archive(project_id: str, archived: bool = True) -> dict[str, Any]:
        return await runtime.service.project_archive(
            runtime.requester(),
            project_id=project_id,
            archived=archived,
        )

    @mcp.tool(name="project_delete")
    async def project_delete(project_id: str) -> dict[str, Any]:
        return await runtime.service.project_delete(runtime.requester(), project_id=project_id)

    @mcp.tool(name="memory_search")
    async def memory_search(search_query: str, target: str = "") -> dict[str, str]:
        return await runtime.service.memory_search(
            runtime.requester(),
            search_query=search_query,
            target=target,
        )

    @mcp.tool(name="record_finding")
    async def record_finding(
        project: str,
        content: str,
        status: str = "",
        agent: str = "",
        observed_at: str = "",
    ) -> dict[str, str]:
        return await runtime.service.record_finding(
            runtime.requester(),
            project=project,
            content=content,
            status=status,
            agent=agent,
            observed_at=observed_at,
        )

    @mcp.tool(name="record_decision")
    async def record_decision(
        project: str,
        content: str,
        status: str = "",
        agent: str = "",
        observed_at: str = "",
    ) -> dict[str, str]:
        return await runtime.service.record_decision(
            runtime.requester(),
            project=project,
            content=content,
            status=status,
            agent=agent,
            observed_at=observed_at,
        )

    @mcp.tool(name="remember")
    async def remember(
        content: str,
        target: str = "",
        agent: str = "",
        observed_at: str = "",
    ) -> dict[str, str]:
        return await runtime.service.remember(
            runtime.requester(),
            content=content,
            target=target,
            agent=agent,
            observed_at=observed_at,
        )

    @mcp.tool(name="forget")
    async def forget(
        project_id: str,
        query: str,
        confirm: bool = False,
        conclusion_ids: list[str] | None = None,
    ) -> dict[str, str]:
        return await runtime.service.forget(
            runtime.requester(),
            project_id=project_id,
            query=query,
            confirm=confirm,
            conclusion_ids=conclusion_ids or [],
        )

    @mcp.tool(name="correct")
    async def correct(
        project_id: str,
        query: str,
        replacement: str,
        confirm: bool = False,
        conclusion_ids: list[str] | None = None,
        observed_at: str = "",
    ) -> dict[str, str]:
        return await runtime.service.correct(
            runtime.requester(),
            project_id=project_id,
            query=query,
            replacement=replacement,
            confirm=confirm,
            conclusion_ids=conclusion_ids or [],
            observed_at=observed_at,
        )

    @mcp.tool(name="open_thread")
    async def open_thread(project_id: str, title: str = "") -> dict[str, Any]:
        return await runtime.service.open_thread(
            runtime.requester(),
            project_id=project_id,
            title=title,
        )

    @mcp.tool(name="archive_thread")
    async def archive_thread(project_id: str, thread_id: str, reason: str = "") -> dict[str, Any]:
        return await runtime.service.archive_thread(
            runtime.requester(),
            project_id=project_id,
            thread_id=thread_id,
            reason=reason,
        )

    @mcp.tool(name="unarchive_thread")
    async def unarchive_thread(project_id: str, thread_id: str) -> dict[str, Any]:
        return await runtime.service.unarchive_thread(
            runtime.requester(),
            project_id=project_id,
            thread_id=thread_id,
        )

    @mcp.tool(name="send_turn")
    async def send_turn(project_id: str, thread_id: str, text: str) -> dict[str, Any]:
        return await runtime.service.send_turn(
            runtime.requester(),
            project_id=project_id,
            thread_id=thread_id,
            text=text,
        )

    @mcp.tool(name="upload_file")
    async def upload_file(
        project_id: str = "",
        path: str = "",
        url: str = "",
        content: str = "",
        filename: str = "",
        title: str = "",
        artifact_type: str = "spec",
        agent: str = "",
    ) -> dict[str, Any]:
        return await runtime.service.upload_file(
            runtime.requester(),
            project_id=project_id,
            path=path,
            url=url,
            content=content,
            filename=filename,
            title=title,
            artifact_type=artifact_type,
            agent=agent,
        )

    @mcp.tool(name="retract_file")
    async def retract_file(project_id: str, doc_id: str) -> dict[str, Any]:
        return await runtime.service.retract_file(
            runtime.requester(),
            project_id=project_id,
            doc_id=doc_id,
        )

    @mcp.tool(name="project_list_files")
    async def project_list_files(project_id: str, include_retracted: bool = False) -> dict[str, Any]:
        return await runtime.service.project_list_files(
            runtime.requester(),
            project_id=project_id,
            include_retracted=include_retracted,
        )

    return mcp


def run_stdio(cfg: Config, *, token: str) -> None:
    store = MCPTokenStore(cfg.mcp_serve.token_store_path)
    service = JarvisMCPService(cfg)
    record = store.resolve(token)
    if record is None:
        raise MCPAccessError("invalid or revoked MCP token")
    ctx = service.context_for_principal(record.principal)
    runtime = MCPServerRuntime(service=service, fixed_context=ctx)
    build_mcp(runtime).run("stdio")


def run_http(cfg: Config) -> None:
    import uvicorn

    service = JarvisMCPService(cfg)
    runtime = MCPServerRuntime(service=service)
    mcp = build_mcp(runtime)
    app = _BearerAuthASGI(mcp.streamable_http_app(), service, cfg)
    bind_host = cfg.mcp_serve.bind_host or cfg.mcp_serve.host
    uvicorn.run(app, host=bind_host, port=cfg.mcp_serve.port, log_level="info")


class _BearerAuthASGI:
    def __init__(self, app: Any, service: JarvisMCPService, cfg: Config, *, http_get: Callable[..., Any] | None = None) -> None:
        self.app = app
        self.service = service
        self.cfg = cfg
        self.store = MCPTokenStore(cfg.mcp_serve.token_store_path)
        self.mode = auth_mode(str(cfg.mcp_serve.auth_mode))
        self.resource_url = cfg.mcp_serve.resolved_resource_url
        self.oauth_issuer = str(cfg.mcp_serve.oauth_issuer).strip()
        self.oauth_scopes = required_scopes(str(cfg.mcp_serve.oauth_required_scopes))
        self.oauth_validator = self._build_oauth_validator(http_get or httpx.get)

    def _build_oauth_validator(self, http_get: Callable[..., Any]) -> OAuthTokenValidator | None:
        if self.mode not in {"oauth", "hybrid"}:
            return None
        missing = [
            name
            for name, value in {
                "issuer": self.oauth_issuer,
                "jwks_url": self.cfg.mcp_serve.oauth_jwks_url,
                "resource_url": self.resource_url,
            }.items()
            if not str(value).strip()
        ]
        if missing:
            logger.warning(
                "mcp-serve OAuth disabled: missing %s",
                ", ".join(missing),
            )
            return None
        try:
            return OAuthTokenValidator(
                issuer=self.oauth_issuer,
                audience=self.resource_url,
                jwks_url=str(self.cfg.mcp_serve.oauth_jwks_url),
                scopes=self.oauth_scopes,
                jarvis_user_claim="",
                default_alg="RS256",
                jwks_ttl_s=float(self.cfg.mcp_serve.oauth_jwks_ttl_s),
                jwks_min_refresh_s=float(self.cfg.mcp_serve.oauth_jwks_min_refresh_s),
                http_get=http_get,
                require_jarvis_user=False,
            )
        except ValueError as exc:
            logger.warning("mcp-serve OAuth disabled: %s", exc)
            return None

    async def __call__(self, scope: dict[str, Any], receive: Callable[[], Awaitable[Any]], send: Callable[[Any], Awaitable[None]]) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        if scope.get("path") == _METADATA_PATH:
            await self._send_metadata(send)
            return
        token = _bearer_token(scope.get("headers") or [])
        try:
            ctx = await self._context_for_token(token)
        except MCPAccessError:
            await _send_plain(send, 403, "principal is not available")
            return
        if ctx is None:
            await self._send_unauthorized(send)
            return
        reset = _REQUEST_CONTEXT.set(ctx)
        try:
            await self.app(scope, receive, send)
        finally:
            _REQUEST_CONTEXT.reset(reset)

    async def _context_for_token(self, token: str) -> RequestContext | None:
        if self.mode in {"legacy", "hybrid"}:
            record = self.store.resolve(token)
            if record is not None:
                try:
                    return self.service.context_for_principal(record.principal)
                except MCPAccessError:
                    if self._challenge_enabled:
                        return None
                    raise
        if self.mode not in {"oauth", "hybrid"} or self.oauth_validator is None:
            return None
        try:
            principal = await asyncio.to_thread(self.oauth_validator.validate, token)
            user = _user_for_oauth_subject(load_users(self.cfg.capabilities.users_dir), principal.subject)
            return self.service.context_for_principal(user.name)
        except (OAuthValidationError, MCPAccessError):
            return None

    async def _send_metadata(self, send: Callable[[Any], Awaitable[None]]) -> None:
        if not self.oauth_issuer or (
            self.mode in {"oauth", "hybrid"} and self.oauth_validator is None
        ):
            await _send_plain(send, 404, "not found")
            return
        await _send_json(
            send,
            200,
            {
                "resource": self.resource_url,
                "authorization_servers": [self.oauth_issuer],
                "bearer_methods_supported": ["header"],
                "resource_name": "Jarvis MCP",
                "scopes_supported": list(self.oauth_scopes),
            },
        )

    async def _send_unauthorized(self, send: Callable[[Any], Awaitable[None]]) -> None:
        await _send_plain(
            send,
            401,
            "unauthorized",
            extra_headers=self._challenge_headers if self._challenge_enabled else None,
        )

    @property
    def _challenge_enabled(self) -> bool:
        return self.oauth_validator is not None

    @property
    def _challenge_headers(self) -> list[tuple[bytes, bytes]]:
        value = f'Bearer resource_metadata="{self.resource_url}{_METADATA_PATH}"'
        return [(b"www-authenticate", value.encode("ascii"))]


def _user_for_oauth_subject(users: dict[str, User], subject: str) -> User:
    matched: dict[str, User] = {}
    for user in users.values():
        if subject in user.oauth_subjects:
            matched[user.name] = user
    for key, user in users.items():
        if subject in {key, user.name}:
            matched[user.name] = user
    if len(matched) > 1:
        logger.error("mcp-serve OAuth subject maps to multiple users subject=%s", subject)
        raise OAuthValidationError("duplicate subject mapping")
    if not matched:
        raise OAuthValidationError("unknown subject")
    return next(iter(matched.values()))


def _bearer_token(headers: list[tuple[bytes, bytes]]) -> str:
    for name, value in headers:
        if name.lower() != b"authorization":
            continue
        text = value.decode("latin1")
        prefix = "Bearer "
        return text[len(prefix) :].strip() if text.startswith(prefix) else ""
    return ""


async def _send_json(send: Callable[[Any], Awaitable[None]], status: int, data: dict[str, Any]) -> None:
    body = json.dumps(data, separators=(",", ":")).encode("utf-8")
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode("ascii")),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


async def _send_plain(
    send: Callable[[Any], Awaitable[None]],
    status: int,
    text: str,
    *,
    extra_headers: list[tuple[bytes, bytes]] | None = None,
) -> None:
    body = text.encode("utf-8")
    headers = [
        (b"content-type", b"text/plain; charset=utf-8"),
        (b"content-length", str(len(body)).encode("ascii")),
    ]
    if extra_headers:
        headers.extend(extra_headers)
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": headers,
        }
    )
    await send({"type": "http.response.body", "body": body})
