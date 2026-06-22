# Room Pi — installing a shared intercom (Phase 3d)

A room Pi is a thin **intercom**: wake word + mic + speaker that phones home to the
brain over a WebSocket. It holds **no credentials and no intelligence** — only a
pairing token. STT/TTS/LLM/memory all run on (or are reached by) the brain. Same
code as the Mac, different `.env`.

## On the brain (your Mac / Hive), once
Give the Pi its own pairing token bound to a device id + profile:

```bash
jarvis pair room-pi --pi-installer --brain-host imac.private
```

The command prints the `BRAIN_DEVICES` entry for the brain and a copy/paste
installer command for the Pi. Add the entry to the brain config before running
the Pi installer.

`profiles/room-pi.md` already ships a house-scoped profile (web.search, files.read,
mcp.context7). A shared device stays **house** scope until a speaker confirms who
they are by voice ("it's Jules") — then that conversation gets their personal scope.

Run the brain: `uv run jarvis brain`.

## On the Pi
```bash
curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/main/scripts/install_pi.sh \
  -o /tmp/install_jarvis_pi.sh
sudo JARVIS_BRAIN_HOST=imac.private \
  JARVIS_INTERCOM_TOKEN=pick-a-long-secret \
  JARVIS_DEVICE_ID=room-pi \
  bash /tmp/install_jarvis_pi.sh
```

Preview the command sequence without root or hardware changes:

```bash
JARVIS_DRY_RUN=1 \
  JARVIS_BRAIN_HOST=imac.private \
  JARVIS_INTERCOM_TOKEN=pick-a-long-secret \
  JARVIS_DEVICE_ID=room-pi \
  bash scripts/install_pi.sh
```

The current development fallback is still `uv sync --extra stt --extra vad
--extra wake` followed by `uv run jarvis run`, but the deployment target is a Pi
installer or image that hides `uv`, writes local config, pairs the device, and
installs the systemd unit.

## Updating and checking the Pi

The installer writes a Pi-specific helper:

```bash
sudo jarvis-pi update
jarvis-pi status
jarvis-pi logs
jarvis-pi doctor
```

`update` refreshes the runtime from the configured public repository/ref, syncs
intercom dependencies, reloads systemd, and restarts the intercom service.
`doctor` prints the configured brain/device, service state, microphone/speaker
enumeration, and camera listing when `libcamera-hello` is available.

See `docs/DEPLOYMENT.md` for the product install flow.

## Notes
- The token is **bound to its device_id**: a leaked Pi token can't impersonate your
  Mac (`authorise_device` rejects a token used from the wrong device).
- Phase 2 relocation is just changing `INTERCOM_BRAIN_HOST` (+ the brain's
  `MEMORY_HOST`) to private-network hostnames — no code changes.
- A personal device instead pins its owner: `{"token":"…","device_id":"local-mac","identity":"neil"}`,
  or bind the device under `users/neil.md` (`devices: [local-mac]`).
