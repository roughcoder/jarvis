# Jarvis MCP ⇄ Cockpit OAuth integration — handover contract

Audience: the `jarvis-cockpit` team. This is the **as-built** contract for
wiring the cockpit to `jarvis mcp-serve` over OAuth. The Jarvis side (this
repo) is implemented and merged (`docs/MCP_SERVE_AUTH.md`, PR #92). This
document is what you verify against and build the cockpit half from.

Dated 2026-07-06. Source of truth is the code; file:line refs are given so you
can confirm anything here against `roughcoder/jarvis` `main`.

## The shape of the integration

Jarvis `mcp-serve` is now a **spec-perfect OAuth 2.1 protected resource** (the
resource-server half of the MCP authorization spec). It does **not** issue
tokens. The **cockpit's Better Auth instance is the authorization server**:
it authenticates the user, issues short-lived JWT access tokens (+ refresh
tokens), and publishes JWKS. Jarvis validates those JWTs locally and maps the
token subject to a Jarvis principal.

```
MCP client (Codex/Claude/…)                Cockpit (Better Auth = AS)      Jarvis mcp-serve (RS)
  |-- discover ------------------------------------------------------------->|  GET /.well-known/oauth-protected-resource
  |<-- authorization_servers: [cockpit] -------------------------------------|
  |-- OAuth 2.1 + PKCE --> /authorize, /token ---->|                         |
  |<-- access_token (JWT, aud=<jarvis resource>), refresh_token -------------|
  |-- MCP call, Authorization: Bearer <JWT> -------------------------------->|  validates via cockpit JWKS
  |<-- tool result (runs under the mapped Jarvis user's capabilities) -------|
  |-- (on 401/expiry) refresh via cockpit /token -->|  silent, client-driven |
```

Token lifetime is the client's problem, solved by refresh tokens — no
long-lived bearer material is planted anywhere.

## What Jarvis provides (as-built)

### MCP endpoint
- Streamable-HTTP MCP at **`/mcp`** on `MCP_SERVE_HOST:MCP_SERVE_PORT`
  (default `localhost:8795`), bind host `MCP_SERVE_BIND_HOST or MCP_SERVE_HOST`
  (`src/jarvis/mcp_server/server.py`).

### Discovery (RFC 9728)
- **`GET /.well-known/oauth-protected-resource`** — unauthenticated JSON:
  ```json
  {
    "resource": "<MCP_SERVE_RESOURCE_URL>",
    "authorization_servers": ["<MCP_SERVE_OAUTH_ISSUER>"],
    "bearer_methods_supported": ["header"],
    "resource_name": "Jarvis MCP",
    "scopes_supported": []
  }
  ```
  `scopes_supported` reflects `MCP_SERVE_OAUTH_REQUIRED_SCOPES` (empty by
  default). Returns **404** when no issuer is configured.
- Every 401 from `/mcp` carries:
  `WWW-Authenticate: Bearer resource_metadata="<resource>/.well-known/oauth-protected-resource"`
  — spec clients bootstrap auth from this with zero prior config.

### Token validation contract (all mandatory in the OAuth lane)
`src/jarvis/oauth.py` (`OAuthTokenValidator`):
- **Signature** against the cockpit JWKS (`MCP_SERVE_OAUTH_JWKS_URL`),
  algorithm pinned from the JWKS key (RS256 default). `alg=none` and
  HS256-with-public-key confusion are rejected by construction.
- **`iss`** equals `MCP_SERVE_OAUTH_ISSUER`.
- **`aud`** must contain the Jarvis resource URL — **exact match within the
  `aud` list**, bound to `MCP_SERVE_RESOURCE_URL` (default
  `http://localhost:8795`). **There is no separate audience env var.** A token
  minted for the cockpit `/v1` API audience is rejected here, and vice versa.
- **`exp`** (required) and **`nbf`** honoured, 30s leeway.
- **`sub`** required (see principal mapping).
- **Scopes**: if `MCP_SERVE_OAUTH_REQUIRED_SCOPES` is set, all must be present.
- JWKS fetched over HTTPS (or localhost); non-secure issuer/JWKS URLs disable
  the lane. Any validation/JWKS error → 401 (fails closed).

### Principal mapping — **this is the main thing the cockpit must get right**
The validated JWT's **`sub`** maps to a Jarvis user
(`src/jarvis/mcp_server/server.py` `_user_for_oauth_subject`,
`src/jarvis/users.py`):
1. **Preferred:** `sub` appears in some `users/<name>.md` front-matter
   `oauth_subjects:` list. Example:
   ```markdown
   ---
   name: neil
   oauth_subjects: ["ba_usr_abc123"]   # Better Auth user id(s)
   ---
   ```
   Works in **all** auth modes.
2. **Fallback (identity):** `sub` exactly equals the user's file stem or
   `name`. Enabled by `MCP_SERVE_OAUTH_ALLOW_IDENTITY_SUBJECT`, which defaults
   **on in hybrid/legacy, off in oauth**.
3. No match → **401**. Never auto-creates a user, never falls back to `house`.
   A `sub` matching two users is a config error → 401.

**Action for the cockpit:** whatever stable subject Better Auth puts in the
access-token `sub` (its opaque user id — do NOT use email or a user-editable
value), that exact string must be listed in the target Jarvis user's
`oauth_subjects`. For an `oauth`-mode deployment this linkage is **required**
(identity fallback is off). The resolved user's Jarvis capabilities gate every
tool call — the MCP surface is deny-by-default and identical to the static
token lane downstream.

### Config surface (Jarvis side, all env, `.env.example` 433-450)
| Env | Default | Meaning |
|---|---|---|
| `MCP_SERVE_HOST` / `MCP_SERVE_PORT` | `localhost` / `8795` | MCP server bind |
| `MCP_SERVE_AUTH_MODE` | `hybrid` | `legacy` \| `oauth` \| `hybrid` |
| `MCP_SERVE_RESOURCE_URL` | `http://{host}:{port}` | Resource id **and required JWT audience**; changing it invalidates outstanding tokens |
| `MCP_SERVE_OAUTH_ISSUER` | empty | Cockpit Better Auth issuer URL |
| `MCP_SERVE_OAUTH_JWKS_URL` | empty | Cockpit JWKS endpoint |
| `MCP_SERVE_OAUTH_REQUIRED_SCOPES` | empty | Optional CSV/space list |
| `MCP_SERVE_OAUTH_ALLOW_IDENTITY_SUBJECT` | `auth_mode != "oauth"` | Allow `sub`==identity fallback |
| `MCP_SERVE_OAUTH_JWKS_TTL_S` / `_MIN_REFRESH_S` | `300` / `30` | JWKS cache |

Auth-mode behaviour: `legacy` = static tokens only; `oauth` = JWT only,
**fails fast at startup** on incomplete OAuth config; `hybrid` (default) =
static token tried first, JWT on miss, degrades to static with one warning if
OAuth config is incomplete. With no OAuth env set, `hybrid` behaves exactly
like today's static-token auth.

### Status projection the cockpit already reads
`GET /v1/mcp/status` `serve` block (`src/jarvis/mcp/status.py`):
```json
"serve": {
  "configured": true,
  "host": "localhost",
  "port": 8795,
  "auth_mode": "hybrid",
  "oauth": {
    "configured": true,
    "issuer": "https://cockpit.example",
    "resource": "http://localhost:8795",
    "metadata_url": "http://localhost:8795/.well-known/oauth-protected-resource"
  },
  "tokens": {"active": 1, "revoked": 0},
  "codex_wired": false,
  "codex_wired_reason": "worker Codex sessions do not currently inject the Jarvis MCP serve endpoint"
}
```
`oauth.configured` is true only when the mode enables OAuth **and** issuer +
JWKS + resource are set **and** the URLs pass the security check. Issuer /
resource / metadata URLs are operator config, not secrets. `codex_wired` is
`false` today (Jarvis-worker-launched Codex sessions do not yet inject this
endpoint — a separate follow-up).

## What the cockpit team builds (and verifies)

This is the AS half plus client wiring. None of it is in the Jarvis repo.

1. **Enable Better Auth as an OAuth provider for MCP.** Better Auth ships the
   `oauth-provider` / `mcp` plugins: authorization + token endpoints, PKCE,
   dynamic client registration, refresh tokens, published JWKS. Configure it to
   mint access tokens whose **`aud` contains the Jarvis `MCP_SERVE_RESOURCE_URL`**
   and whose **`sub` is the stable Better Auth user id** you will list in
   Jarvis `oauth_subjects`.
2. **Point Jarvis at the cockpit AS.** Set on the Jarvis deployment:
   `MCP_SERVE_AUTH_MODE=hybrid` (or `oauth`), `MCP_SERVE_OAUTH_ISSUER=<better-auth base>`,
   `MCP_SERVE_OAUTH_JWKS_URL=<better-auth jwks>`, and `MCP_SERVE_RESOURCE_URL`
   to the canonical Jarvis MCP URL, then add each operator's Better Auth `sub`
   to their `users/<name>.md` `oauth_subjects`.
3. **Register the Jarvis MCP server in agent sessions.** Reuse the existing
   `t3-code` injection plumbing (cockpit `CodexAdapter`): add an entry
   `mcp_servers.jarvis.url = <resource>/mcp`. The client (Codex `codex mcp
   login`, Claude-family) performs discovery → the cockpit OAuth flow → and
   holds/refreshes its own tokens. **No bearer env var, no cockpit token
   minting** for this lane — that's the whole point of going spec-perfect.
4. **Surface status.** The `/v1/mcp/status` `serve.oauth` block is live; the
   cockpit `JarvisMcp.tsx` panel can stop describing the token endpoint as
   "not available" and show real OAuth-configured state.

Keep this separate from the cockpit's own native `t3-code` MCP server (the
browser `preview_*` toolkit injected with a short-lived per-session bearer) —
they are two different MCP servers and the plan already keeps them distinct.

## Verification checklist (end-to-end)

- `GET <resource>/.well-known/oauth-protected-resource` returns the 5-field
  JSON and lists the cockpit issuer.
- An MCP call with no token → 401 + `WWW-Authenticate` pointing at the metadata
  URL.
- A Better-Auth-minted JWT with `aud` = Jarvis resource and `sub` linked via
  `oauth_subjects` → tool call succeeds and runs under that user's
  capabilities.
- A JWT minted for the cockpit `/v1` API audience → **rejected** at `/mcp`.
- Token expiry → client refreshes silently; no operator action.
- (Reference client) `jarvis mcp login` against the cockpit AS completes the
  same flow — usable as a Jarvis-side smoke test.

## Known follow-ups (not blockers for cockpit verification)
- `codex_wired` stays `false` until Jarvis-worker-launched Codex sessions also
  inject this endpoint (worker lane, uses the static `MCPTokenStore` fallback).
- The static token lane (`/v1/mcp/tokens`, `jarvis mcp-serve add-token`) remains
  for headless/non-interactive clients that can't do interactive OAuth.

## Pointers
- Jarvis spec: `docs/MCP_SERVE_AUTH.md`.
- Status projection + `/v1/mcp/*`: `docs/COCKPIT_API.md` (MCP section).
- Code: `src/jarvis/mcp_server/server.py`, `src/jarvis/oauth.py`,
  `src/jarvis/mcp/status.py`, `src/jarvis/users.py`, `src/jarvis/config.py`
  (`MCPServeConfig`).
