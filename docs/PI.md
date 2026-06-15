# Room Pi — installing a shared intercom (Phase 3d)

A room Pi is a thin **intercom**: wake word + mic + speaker that phones home to the
brain over a WebSocket. It holds **no credentials and no intelligence** — only a
pairing token. STT/TTS/LLM/memory all run on (or are reached by) the brain. Same
code as the Mac, different `.env`.

## On the brain (your Mac / Hive), once
Give the Pi its own pairing token bound to a device id + profile:

```bash
# .env on the brain
BRAIN_DEVICES=[{"token":"pick-a-long-secret","device_id":"room-pi"}]
```

`profiles/room-pi.md` already ships a house-scoped profile (web.search, files.read,
mcp.context7). A shared device stays **house** scope until a speaker confirms who
they are by voice ("it's Jules") — then that conversation gets their personal scope.

Run the brain: `uv run jarvis brain`.

## On the Pi
```bash
uv sync --extra stt --extra vad --extra wake   # thin edge: no gateway/tts/memory keys
cp .env.example .env
# point it at the brain and identify as the room device:
#   INTERCOM_BRAIN_HOST=<brain-host>     (Tailscale name in Phase 2)
#   INTERCOM_TOKEN=pick-a-long-secret    (must match this device's BRAIN_DEVICES token)
#   CAPS_DEVICE_ID=room-pi               (selects profiles/room-pi.md on the brain)
uv run jarvis status      # brain reachable? what am I allowed to do?
uv run jarvis run         # become an intercom
```

## Notes
- The token is **bound to its device_id**: a leaked Pi token can't impersonate your
  Mac (`authorise_device` rejects a token used from the wrong device).
- Phase 2 relocation is just changing `INTERCOM_BRAIN_HOST` (+ the brain's
  `MEMORY_HOST`) to Tailscale hostnames — no code changes.
- A personal device instead pins its owner: `{"token":"…","device_id":"local-mac","identity":"neil"}`,
  or bind the device under `users/neil.md` (`devices: [local-mac]`).
