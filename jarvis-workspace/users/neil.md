---
# Neil — primary user. The Mac (local-mac) is his personal device, so on it he's
# resolved with STRONG confidence → personal scope automatically. `capabilities`
# are his own grants, added on top of the device profile when he's in personal
# scope. `credentials` are references only — never secrets (those live in .env).
devices: [local-mac]
whatsapp: []
claims: ["it's neil", "this is neil", "neil here"]
capabilities: [mcp.context7, mcp.obsidian, mcp.granola, mcp.notion, mcp.linear]
scope: personal
honcho_peer: neil
---

# Neil

Primary household user. Identified strongly on his own Mac; on shared devices
(the room Pi) he's identified by voice claim. Memory is scoped to the `neil`
Honcho peer; MCP tokens to `.mcp-auth/neil/` (see WS2).
