---
# Jules — second household user. No personal device of her own yet, so she's
# identified by voice claim ("it's Jules") on shared devices → claimed confidence,
# personal scope (family-grade). Her grants and memory peer are entirely separate
# from Neil's (the privacy wall, §5).
devices: []
whatsapp: []
claims: ["it's jules", "this is jules", "jules here"]
capabilities: [mcp.context7, mcp.granola]
scope: personal
honcho_peer: jules
---

# Jules

Second household user. Identified by voice claim on shared devices. Memory scoped
to the `jules` Honcho peer; her MCP tokens to `.mcp-auth/jules/`. Never sees
Neil's data, and her tokens are never used for his requests.
