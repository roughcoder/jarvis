---
# Capability profile for the WhatsApp channel (device_id "whatsapp").
# This is the device CEILING for the WhatsApp connector: what is permitted for
# anyone who messages the bot after pairing.
#
# DELIBERATELY EXCLUDED (remote channel, higher blast radius than the Mac):
#   - worker.shell / worker.gui  → no running shell or driving the Mac's screen
#     from a WhatsApp message (the biggest risk if a phone/WhatsApp is taken over)
#   - files.write / worker.code  → no writing files or kicking off coding jobs
#   - skills.author              → can run skills, not author new ones
#
# Personal accounts stay per-identity in ignored user files. google.* here is the
# shared house account; move it to user files if that is too broad for a fleet.
capabilities:
  - web.search
  - files.read
  - worker.browser
  - background.run
  - alarms.set
  - google.read
  - google.send
  - profile.write
  - skills.run
  - mcp.context7
---

# whatsapp — the WhatsApp channel

The remote-channel ceiling: it can browse the web, research, run background
tasks, set alarms, use the house Google account, read workspace files, use
public-doc MCP (context7), and remember personal facts. It cannot run shell,
drive the Mac's screen, write files, or author skills from here.

`worker.browser` and `background.run` need the worker daemon running
(`jarvis worker`); without it those tools return "worker unreachable" rather than
acting. `google.*` is Jarvis's own (house) Google account, shared by every paired
user on this channel — narrow it (move google to user files) if that's too broad.
