---
# Capability profile for a shared room intercom (a Raspberry Pi). It's a SHARED
# device, so by default speakers are unknown → house scope. These capabilities are
# the device ceiling: house-safe lookups only. A confirmed speaker ("it's Jules")
# adds their own grants on top (in personal scope); an unknown speaker gets just
# these. No account/personal tools here by default.
capabilities:
  - web.search
  - files.read
  - mcp.context7
---

# room-pi — shared household intercom

A thin intercom (wake + mic + speaker) that phones home to the brain. Holds no
credentials; pairs with its own token (BRAIN_DEVICES). Identity is house until a
speaker confirms who they are by voice, which upgrades scope for that conversation.
