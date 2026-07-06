# MCP Serve OAuth — spec

Status: implemented. Companion to
`docs/COCKPIT_API.md` (MCP section) and the MCP authorization specification.
Dated 2026-07-06.

## Problem

`jarvis mcp-serve` exposes Jarvis as an MCP server over streamable HTTP, but its
only authentication is a private lane of static per-principal bearer tokens
(`MCPTokenStore`), minted via `jarvis mcp-serve add-token` or
`POST /v1/mcp/tokens`. That lane has three problems for the cockpit era:

1. **Credential lifetime.** Static tokens live until revoked. Injecting one
   into an agent session (e.g. Codex `bearer_token_env_var`) plants a
   long-lived credential in process env for the life of the session.
2. **Manual provisioning.** Every principal × client pairing needs an operator
   to mint and distribute a token out of band.
3. **Not the MCP spec.** Spec-compliant MCP clients (Codex `codex mcp login`,
   Claude-family clients, Jarvis's own `jarvis mcp login`) expect the OAuth
   2.1 authorization flow with discovery. Any divergence from the spec breaks
   those clients or forces per-client workarounds.

Meanwhile the cockpit already operates an OAuth-capable identity plane (Better
Auth, which can act as an OAuth provider for MCP clients: authorization +
token endpoints, PKCE, dynamic client registration, refresh tokens, published
JWKS), and the Jarvis orchestration API already validates JWTs locally via
cached JWKS (`OAuthTokenValidator`).

## Decision

`jarvis mcp-serve` becomes a **spec-perfect OAuth 2.1 protected resource**
(the resource-server half of the MCP authorization specification). Jarvis does
**not** build an authorization server: the AS is external and configured by
env — first deployment is the cockpit's Better Auth instance.

Token lifetime is the spec's problem, not ours: access tokens stay short-lived
and MCP clients refresh them silently via refresh tokens. No long-lived bearer
material is issued for the OAuth lane.

The static token lane is retained as a **fallback** for non-interactive
clients (worker-spawned sessions, scripts). Both lanes terminate in the same
place: a Jarvis `users/` principal and the deny-by-default capability gate.

### Non-goals

- No authorization server, no `/authorize`, no `/token`, no dynamic client
  registration inside Jarvis. Those belong to the configured AS.
- No change to cockpit `/v1` API auth (`ORCHESTRATION_AUTH_MODE` et al.).
- No removal of `MCPTokenStore`, the CLI token commands, or `/v1/mcp/tokens`.
- No stdio-transport auth changes (`run_stdio` keeps its explicit token).

## Auth modes

New env `MCP_SERVE_AUTH_MODE`, mirroring the orchestration API's model:

| Mode | Behaviour |
|---|---|
| `legacy` | Static `MCPTokenStore` bearer tokens only (today's behaviour). |
| `oauth` | JWT bearer tokens only, validated per this spec. |
| `hybrid` (default) | Try static token resolution first; if the presented bearer is not a known static token, validate it as a JWT. |

In `hybrid` mode with incomplete OAuth config (missing issuer, JWKS URL, or
resource URL), OAuth validation is disabled and startup logs a single warning
only when at least one OAuth env var was set; static-token behaviour remains.
In `oauth` mode, incomplete or invalid OAuth config is a startup error: the
server must fail fast with a clear message rather than booting into total
lockout or silently falling back to static tokens. With no new env set, default
`hybrid` mode must not log an OAuth warning and must behave like today's
static-token auth.

Unknown `MCP_SERVE_AUTH_MODE` values are configuration errors and must be
rejected at startup; they must not silently coerce to `hybrid`.

Ordering note for `hybrid`: static tokens are random opaque strings and JWTs
are three dot-separated base64url segments — but resolution MUST NOT rely on
shape sniffing. The static store lookup is a constant-time hash comparison
against known tokens; only on a miss does JWT validation run. A JWT can never
collide with a stored static token hash.

## Discovery (RFC 9728)

`mcp-serve` publishes OAuth protected resource metadata:

```text
GET /.well-known/oauth-protected-resource
```

When `MCP_SERVE_RESOURCE_URL` contains a path, the RFC 9728 metadata path is
also served with the protected resource path appended after the well-known
segment, for example:

```text
GET /.well-known/oauth-protected-resource/mcp/serve
```

Unauthenticated, `Content-Type: application/json`:

```json
{
  "resource": "<MCP_SERVE_RESOURCE_URL>",
  "authorization_servers": ["<MCP_SERVE_OAUTH_ISSUER>"],
  "bearer_methods_supported": ["header"],
  "resource_name": "Jarvis MCP",
  "scopes_supported": []
}
```

- `resource` is the canonical resource identifier — see **Resource URL**
  below.
- `scopes_supported` is empty in v1: authorization is Jarvis's capability
  gate, not OAuth scopes. If `MCP_SERVE_OAUTH_REQUIRED_SCOPES` is set, list
  those scopes here.
- Served in all auth modes whenever `MCP_SERVE_OAUTH_ISSUER` is configured,
  even if the OAuth validation lane is disabled in degraded `hybrid` mode.
  The route returns `404` only when no issuer is configured (there is no AS to
  advertise).

### 401 challenge

Every unauthenticated or failed-auth response to the MCP endpoint returns
`401` with the spec's discovery pointer:

```text
WWW-Authenticate: Bearer resource_metadata="<resource-origin>/.well-known/oauth-protected-resource[/resource-path]"
```

This is what lets a spec client bootstrap auth with zero prior configuration.
For a resource URL of `https://jarvis.example/mcp/serve`, the challenge points
to `https://jarvis.example/.well-known/oauth-protected-resource/mcp/serve`.
The static-token lane's failures return the same challenge in `hybrid`/`oauth`
mode (the client can't tell which lane rejected it, and shouldn't).

## Token validation

Reuse the existing `OAuthTokenValidator` (`src/jarvis/orchestration/oauth.py`)
rather than writing a second validator. Checks, all mandatory in the OAuth
lane:

1. **Signature** against the cached JWKS (`MCP_SERVE_OAUTH_JWKS_URL`), same
   TTL/min-refresh behaviour as the orchestration validator.
2. **Issuer** equals `MCP_SERVE_OAUTH_ISSUER`.
3. **Audience** contains the canonical resource URL (RFC 8707 binding). A
   token minted for the cockpit `/v1` API audience MUST be rejected here, and
   vice versa. Audience validation is not optional in any mode.
4. **Time claims** (`exp`, `nbf` with small skew) — already enforced by the
   validator.
5. **Scopes**: if `MCP_SERVE_OAUTH_REQUIRED_SCOPES` is non-empty, all listed
   scopes must be present. Default empty.

No token introspection (RFC 7662) in v1: the AS must issue JWT access tokens
verifiable via JWKS (Better Auth's OAuth provider does). If a future AS issues
opaque tokens, introspection becomes a follow-up spec.

## Principal mapping

The validated JWT's `sub` maps to a Jarvis user:

1. Each user profile under `users/` MAY declare an `oauth_subjects:` list in
   front matter (e.g. `oauth_subjects: ["ba_usr_abc123"]`). A `sub` matching
   any entry resolves to that user.
2. Fallback, only when no `oauth_subjects` entry matched and
   `MCP_SERVE_OAUTH_ALLOW_IDENTITY_SUBJECT` allows it: a `sub` exactly equal to
   a user's identity key (file name / `name`) resolves to that user. This
   preserves today's cockpit-API behaviour where `sub` is the Jarvis identity.
3. No match → `401` with the challenge header. **Never** auto-create users,
   never fall back to `house`.

The identity fallback is controlled by `MCP_SERVE_OAUTH_ALLOW_IDENTITY_SUBJECT`.
When unset, it defaults to enabled in `legacy`/`hybrid` mode and disabled in
`oauth` mode, so an OAuth-only cutover requires explicit `oauth_subjects`
linkage unless an operator deliberately enables the fallback. If set, the flag
overrides the mode-derived default.

A `sub` matching more than one user within the active mapping lane is a
configuration error: log and reject (`401`). `oauth_subjects` matches take
precedence over identity fallback, so a subject that is explicitly linked to
one user and also happens to equal another user's identity resolves to the
explicitly linked user. The resolved user builds the same per-principal
capability context the static-token lane builds today (`user.capabilities`,
deny by default) — the MCP tool surface downstream of auth is unchanged.

The orchestration validator's `jarvis_user` claim handling is NOT used here;
`sub` is the only identity anchor (consistent with the AUTH-ONLY V1 rule that
custom claims are untrusted).

## Resource URL

New env `MCP_SERVE_RESOURCE_URL` — the canonical `https?://host:port` clients
use to reach this server (no trailing slash; path allowed if the MCP endpoint
is mounted under one). Default: derived as
`http://{MCP_SERVE_HOST}:{MCP_SERVE_PORT}`.

This value is load-bearing twice: it is the RFC 9728 `resource` value and the
required JWT audience. Phase-2 relocation (Tailscale hostname) is an env
change, per the repo's network-boundary constraint. Document in
`.env.example` that changing it invalidates outstanding access tokens (they
carry the old audience) — clients recover by refreshing.

## Config summary

All new fields on `MCPServeConfig` (env prefix `MCP_SERVE_`), with
`.env.example` lines:

| Env | Default | Meaning |
|---|---|---|
| `MCP_SERVE_AUTH_MODE` | `hybrid` | `legacy` \| `oauth` \| `hybrid` |
| `MCP_SERVE_RESOURCE_URL` | derived from host/port | Canonical resource id + required audience |
| `MCP_SERVE_OAUTH_ISSUER` | empty | AS issuer URL (Better Auth base URL) |
| `MCP_SERVE_OAUTH_JWKS_URL` | empty | AS JWKS endpoint |
| `MCP_SERVE_OAUTH_REQUIRED_SCOPES` | empty | CSV, optional |
| `MCP_SERVE_OAUTH_ALLOW_IDENTITY_SUBJECT` | true in `legacy`/`hybrid`, false in `oauth` | Allow `sub` identity fallback when no `oauth_subjects` entry matches |
| `MCP_SERVE_OAUTH_JWKS_TTL_S` / `_MIN_REFRESH_S` | as orchestration | JWKS cache tuning |

`jarvis config` must print the resolved values (secret-free — none of these
are secrets).

Deployments that enable OAuth for `jarvis mcp-serve` install the `mcp` extra;
that extra includes `PyJWT[crypto]` for local JWKS validation.

## Status surface

`GET /v1/mcp/status` `serve` block gains:

```json
"serve": {
  "configured": true,
  "auth_mode": "hybrid",
  "oauth": {
    "configured": true,
    "issuer": "https://cockpit.example",
    "resource": "http://jarvis.local:8795",
    "metadata_url": "http://jarvis.local:8795/.well-known/oauth-protected-resource"
  },
  ...existing fields...
}
```

Issuer and resource URLs here are operator-configured endpoints, not secrets;
they are exempt from URL redaction the same way `serve.host`/`serve.port`
already are. Nothing else about redaction changes.

The `oauth.configured` field is true only when the OAuth lane can be built with
the same issuer/JWKS URL security validation used by the runtime validator.
Invalid non-localhost `http://` issuer or JWKS URLs therefore report
`configured: false` even when values are present.

## Client flows (documentation, not code)

- **Codex**: register once — `codex mcp add jarvis --url <resource>/mcp` then
  `codex mcp login jarvis`. Codex performs discovery → AS flow → stores and
  refreshes tokens itself. The cockpit's Codex adapter can write this config
  into sessions it launches; no bearer env var, no cockpit token minting.
- **Claude-family**: standard remote MCP server entry; interactive OAuth on
  first use.
- **Jarvis as its own client** (`jarvis mcp login`): already implements this
  flow; usable as an end-to-end smoke test against a local Better Auth.
- **Headless/worker lane**: static token via `MCP_SERVE_AUTH_MODE=hybrid`,
  unchanged.

## Security invariants

- Deny by default end to end: unknown `sub` → 401; resolved user's capability
  set gates every tool call exactly as today.
- Audience binding is mandatory whenever OAuth validation runs — no config
  flag may disable it.
- JWTs are never written to the token store, logs, or idempotency records;
  static plaintext tokens likewise never logged (unchanged).
- JWKS fetch failures fail closed for the OAuth lane (reject with 401 +
  challenge) and MUST NOT take down the static lane in `hybrid` mode.
- The metadata route and challenge header leak only operator-configured
  endpoint URLs — nothing derived from runtime state.

## Testing requirements

Unit tests (no network): a local RS256 keypair fixture minting JWTs, JWKS
served from a stub.

- Discovery: metadata shape; metadata served in legacy-with-issuer; 404 with
  no issuer; challenge header on 401 for missing/invalid/expired tokens.
- Validation matrix: happy path; wrong issuer; wrong audience (API-audience
  token rejected); expired; not-before; bad signature; scope enforcement when
  configured.
- Principal mapping: `oauth_subjects` match; `oauth_subjects` precedence over
  identity fallback; identity fallback allowed/disabled by mode/config; unknown
  sub rejected; duplicate mapping rejected; capability context matches the same
  user via the static lane.
- Hybrid ordering: static token still resolves when OAuth configured; JWT
  works alongside; garbage bearer rejected by both lanes with one 401.
- Degradation: partial OAuth config disables the lane with a warning in
  `hybrid` mode and leaves static lane unaffected; the same partial config
  fails fast in `oauth` mode.
- `jarvis config` prints new fields; `/v1/mcp/status` serve block projection.

## Rollout

Backward compatible: with no new env set, `hybrid` mode behaves exactly like
today's static-token auth. Commit trailers must carry `Env:` lines for every
new variable and a `Release-note:` describing the OAuth lane. The follow-up
(separate spec, delivered by the cockpit team) enables Better Auth's OAuth
provider/MCP plugin and registers the Jarvis server in cockpit-launched agent
sessions.
