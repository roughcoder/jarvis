---
# Capability profile for this Mac (CAPS_DEVICE_ID defaults to "local-mac").
# This is the full-trust device: `jarvis run --local` (and a paired Mac intercom)
# resolves its capabilities from here. A profile file OVERRIDES the .env
# CAPS_DEFAULT_CAPABILITIES fallback entirely, so list everything the Mac should
# have. Other intercoms (a room Pi, etc.) get their own, narrower profile; a
# device with no profile file falls back to the CSV default.
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
  - mcp.context7
  - mcp.obsidian
  - mcp.granola
  - mcp.notion
  - mcp.linear
---

# local-mac — Neil's Mac (full scope)

Everything Jarvis can do locally is granted on this device.

When you add an MCP server to `MCP_SERVERS` in `.env`, also add its
`mcp.<name>` capability to the list above — otherwise the bridge will connect
the server and discover its tools, but the deny-by-default gate will keep them
hidden from the model on this device (the firewall against tool sprawl).
