"""FastMCP transport wrapper for Jarvis as an MCP server."""

from __future__ import annotations

import contextvars
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from jarvis.brain.capabilities import RequestContext
from jarvis.config import Config
from jarvis.mcp_server.adapters import JarvisMCPService, MCPAccessError
from jarvis.mcp_server.tokens import MCPTokenStore

_REQUEST_CONTEXT: contextvars.ContextVar[RequestContext | None] = contextvars.ContextVar(
    "jarvis_mcp_request_context",
    default=None,
)


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
        query: str,
        target: str = "",
        confirm: bool = False,
        conclusion_ids: list[str] | None = None,
    ) -> dict[str, str]:
        return await runtime.service.forget(
            runtime.requester(),
            query=query,
            target=target,
            confirm=confirm,
            conclusion_ids=conclusion_ids or [],
        )

    @mcp.tool(name="correct")
    async def correct(
        query: str,
        replacement: str,
        target: str = "",
        confirm: bool = False,
        conclusion_ids: list[str] | None = None,
        observed_at: str = "",
    ) -> dict[str, str]:
        return await runtime.service.correct(
            runtime.requester(),
            query=query,
            replacement=replacement,
            target=target,
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
    app = _BearerAuthASGI(mcp.streamable_http_app(), service, cfg.mcp_serve.token_store_path)
    bind_host = cfg.mcp_serve.bind_host or cfg.mcp_serve.host
    uvicorn.run(app, host=bind_host, port=cfg.mcp_serve.port, log_level="info")


class _BearerAuthASGI:
    def __init__(self, app: Any, service: JarvisMCPService, token_store_path: str) -> None:
        self.app = app
        self.service = service
        self.store = MCPTokenStore(token_store_path)

    async def __call__(self, scope: dict[str, Any], receive: Callable[[], Awaitable[Any]], send: Callable[[Any], Awaitable[None]]) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        token = _bearer_token(scope.get("headers") or [])
        record = self.store.resolve(token)
        if record is None:
            await _send_plain(send, 401, "unauthorized")
            return
        try:
            ctx = self.service.context_for_principal(record.principal)
        except MCPAccessError:
            await _send_plain(send, 403, "principal is not available")
            return
        reset = _REQUEST_CONTEXT.set(ctx)
        try:
            await self.app(scope, receive, send)
        finally:
            _REQUEST_CONTEXT.reset(reset)


def _bearer_token(headers: list[tuple[bytes, bytes]]) -> str:
    for name, value in headers:
        if name.lower() != b"authorization":
            continue
        text = value.decode("latin1")
        prefix = "Bearer "
        return text[len(prefix) :].strip() if text.startswith(prefix) else ""
    return ""


async def _send_plain(send: Callable[[Any], Awaitable[None]], status: int, text: str) -> None:
    body = text.encode("utf-8")
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"content-length", str(len(body)).encode("ascii")),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})
