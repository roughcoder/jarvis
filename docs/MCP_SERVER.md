# Jarvis MCP Server

`jarvis mcp-serve` exposes Jarvis brain powers to external agents through MCP.
It is the inverse of `jarvis mcp`: Jarvis is the server, not the client.

## Auth Model

There is no anonymous mode. Create a revocable token for a real user profile:

```bash
uv run jarvis mcp-serve add-token --principal neil --name "Claude Code"
uv run jarvis mcp-serve list-tokens
uv run jarvis mcp-serve revoke-token mcptok_...
```

Tokens are stored hashed at `MCP_SERVE_TOKEN_STORE_PATH`. A token maps to a
`users/<principal>.md` profile, then Jarvis builds that principal's normal
`RequestContext`; memory and project access use the shared capability gates.

## Serve

Streamable HTTP:

```bash
uv run jarvis mcp-serve serve --transport http
```

Clients connect to `http://<MCP_SERVE_HOST>:<MCP_SERVE_PORT>/mcp` with:

```text
Authorization: Bearer <token>
```

Stdio is one principal per process:

```bash
JARVIS_MCP_TOKEN=<token> uv run jarvis mcp-serve serve --transport stdio
```

## Tools

- `project_list`, `project_get`
- `memory_search`
- `record_finding`, `record_decision`, `remember`
- `open_thread`, `send_turn`
- `upload_file`

`upload_file` is currently a clear not-available stub because the file-vault
ingestion flow is not in this branch.

Writes from MCP force `channel: mcp`, `source: mcp`, `recorded_by` from the
token principal, and an `agent` tag so external-agent conclusions are auditable.
