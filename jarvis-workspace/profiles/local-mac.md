---
# Capability profile for a Mac that can run local Jarvis roles.
# This is the device CEILING: what is permitted on this machine for any resolved
# speaker. Keep personal account capabilities in private user files, not here.
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

# local-mac — example Mac profile

This profile is safe as a public example because it grants only role and
house-level capabilities. Personal MCP servers, private calendars, and account
specific tools belong in ignored `jarvis-workspace/users/*.md` files.
