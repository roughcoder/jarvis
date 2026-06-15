"""OAuth for HTTP MCP servers (Phase 3 §6) — token store + loopback browser flow.

HTTP MCP servers (Notion, Granola, Linear, Microsoft-365) authenticate with
OAuth 2.0. The interactive browser popup happens ONLY in `jarvis mcp login`,
never in the voice loop: the brain builds a *headless* provider (no redirect /
callback handlers) that silently refreshes a cached token or — if fresh auth is
needed — fails to connect, so the bridge skips that server and logs "run
jarvis mcp login". Tokens persist per-server under `<auth_dir>/<server>.json`, so
login is a one-time step and survives restarts.

The `mcp` SDK supplies the whole OAuth machinery (PKCE, dynamic client
registration, RFC 8414 metadata discovery, refresh). We provide only the token
storage and the localhost loopback that catches the redirect. SDK imports are
lazy, like the rest of `mcp/`.
"""

from __future__ import annotations

import asyncio
import json
import pathlib
from typing import Any

from jarvis.config import MCPConfig, MCPServerSpec


class FileTokenStorage:
    """SDK `TokenStorage` backed by one JSON file per server: the OAuth tokens
    plus the dynamically-registered client info (so re-auth reuses the client)."""

    def __init__(self, path: pathlib.Path) -> None:
        self._path = path

    def _read(self) -> dict[str, Any]:
        if self._path.exists():
            try:
                return json.loads(self._path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001 - a corrupt file => treat as empty
                return {}
        return {}

    def _write(self, data: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        try:
            self._path.chmod(0o600)  # tokens are secrets
        except OSError:
            pass

    def has_tokens(self) -> bool:
        return bool(self._read().get("tokens"))

    async def get_tokens(self):  # noqa: ANN201
        from mcp.shared.auth import OAuthToken

        data = self._read().get("tokens")
        return OAuthToken.model_validate(data) if data else None

    async def set_tokens(self, tokens) -> None:  # noqa: ANN001
        data = self._read()
        data["tokens"] = tokens.model_dump(mode="json")
        self._write(data)

    async def get_client_info(self):  # noqa: ANN201
        from mcp.shared.auth import OAuthClientInformationFull

        data = self._read().get("client_info")
        return OAuthClientInformationFull.model_validate(data) if data else None

    async def set_client_info(self, client_info) -> None:  # noqa: ANN001
        data = self._read()
        data["client_info"] = client_info.model_dump(mode="json")
        self._write(data)


class LoopbackFlow:
    """The interactive half of OAuth: open the user's browser at the authorization
    URL, then catch the redirect on a one-shot localhost server. Used only by
    `jarvis mcp login`. `opened` records whether a browser was actually needed (so
    the command can say "authorized" vs "already authorized")."""

    def __init__(self, server_name: str, port: int) -> None:
        self.server_name = server_name
        self.port = port
        self.opened = False

    async def redirect(self, url: str) -> None:
        import webbrowser

        self.opened = True
        print(f"    → opening your browser to authorize {self.server_name}…")
        print(f"      (if it doesn't open, visit:\n       {url} )")
        try:
            webbrowser.open(url)
        except Exception:  # noqa: BLE001 - headless box: the printed URL still works
            pass

    async def callback(self) -> tuple[str, str | None]:
        return await _wait_for_code(self.port)


async def _wait_for_code(port: int) -> tuple[str, str | None]:
    """Serve exactly one request on localhost:<port> and return (code, state) from
    the OAuth redirect. Raises if the user cancelled / the server reported an error."""
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from urllib.parse import parse_qs, urlparse

    captured: dict[str, str | None] = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            q = parse_qs(urlparse(self.path).query)
            captured["code"] = (q.get("code") or [None])[0]
            captured["state"] = (q.get("state") or [None])[0]
            err = (q.get("error") or [None])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            msg = f"Authorization failed: {err}" if err else "Jarvis is authorized — you can close this tab."
            self.wfile.write(
                f"<html><body style='font-family:sans-serif;padding:3em'><h2>{msg}</h2></body></html>".encode()
            )

        def log_message(self, *args: Any) -> None:  # silence the default stderr log
            pass

    server = HTTPServer(("localhost", port), Handler)
    try:
        await asyncio.to_thread(server.handle_request)  # blocks until one request
    finally:
        server.server_close()
    if not captured.get("code"):
        raise RuntimeError("no authorization code received (was the login cancelled?)")
    return captured["code"], captured.get("state")


def auth_path(cfg: MCPConfig, server_name: str, user: str = "house") -> pathlib.Path:
    """Per-`(user, server)` token file (the privacy wall, §5): Jules's tokens live
    under `.mcp-auth/jules/`, Neil's under `.mcp-auth/neil/`, never shared."""
    return pathlib.Path(cfg.auth_dir) / user / f"{server_name}.json"


def needs_oauth(spec: MCPServerSpec) -> bool:
    """An http server with no static auth headers authenticates via OAuth."""
    return spec.transport == "http" and not spec.headers


def build_oauth_provider(  # noqa: ANN201
    spec: MCPServerSpec, cfg: MCPConfig, *, interactive: bool, user: str = "house"
):
    """Build an SDK `OAuthClientProvider` for a `(server, user)`. Interactive => the
    browser loopback flow (for `jarvis mcp login`); headless => no handlers, so the
    provider refreshes that user's cached token or fails fast (the brain never pops a
    browser). Returns (provider, storage, flow|None) — the flow exposes `.opened`."""
    from mcp.client.auth import OAuthClientProvider
    from mcp.shared.auth import OAuthClientMetadata

    storage = FileTokenStorage(auth_path(cfg, spec.name, user))
    redirect_uri = f"http://localhost:{cfg.oauth_redirect_port}/callback"
    metadata = OAuthClientMetadata(
        client_name="Jarvis",
        redirect_uris=[redirect_uri],
        scope=spec.scope or None,
        token_endpoint_auth_method=spec.token_endpoint_auth_method or "none",
    )
    flow = LoopbackFlow(spec.name, cfg.oauth_redirect_port) if interactive else None
    provider = OAuthClientProvider(
        server_url=spec.url,
        client_metadata=metadata,
        storage=storage,
        redirect_handler=flow.redirect if flow else None,
        callback_handler=flow.callback if flow else None,
    )
    return provider, storage, flow
