---
# Capability profile for this Mac (CAPS_DEVICE_ID defaults to "local-mac").
# This is the device CEILING — what's permitted *here*, for ANYONE the device
# resolves to. So it lists only HOUSE-SAFE capabilities: house tools (worker,
# google = Jarvis's own account) and house-safe MCP servers (context7 = public
# docs). PERSONAL servers — Neil's Obsidian vault, his Notion/Linear/Granola
# accounts — are NOT here; they live in `users/neil.md` so they flow with HIS
# identity only, never to another speaker on this device. A profile file
# OVERRIDES the .env CAPS_DEFAULT_CAPABILITIES fallback entirely.
capabilities:
  - web.search
  - files.read
  - files.write
  - worker.code
  - worker.shell
  - skills.run
  - skills.author
  - google.read
  - google.send
  - worker.gui
  - worker.browser
  - background.run
  - alarms.set
  - mcp.context7
---

# local-mac — Neil's Mac

The device ceiling: house-safe capabilities only. Neil's *personal* MCP servers
(`mcp.obsidian`, `mcp.notion`, `mcp.linear`, `mcp.granola`) are granted in
`users/neil.md`, so they attach to Neil's identity rather than to the device —
another speaker resolved on this Mac would not inherit access to his vault or
accounts. When you add a *house-safe* MCP server, grant its `mcp.<name>` here;
a *personal* one goes in the owner's user file.
