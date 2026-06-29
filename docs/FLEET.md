# Jarvis Fleet Deployment

Jarvis runs as a set of local roles. A host may run one role or several roles, but
each role stays independently supervised:

| Host | Roles |
|---|---|
| iMac | `brain`, Docker services, optional `intercom`, optional `worker` |
| Mac laptop | `intercom`, `worker` |
| Raspberry Pi | `intercom` |

`launchd` or `systemd` owns the long-running processes. Operator UIs, including
the Swift menu bar app, observe status and request safe local actions.

## Development vs Installed Mode

There is one Jarvis codebase. We do not maintain separate dev and production
implementations. The distinction is how the same code is run:

| Mode | Owner | Use it for |
|---|---|---|
| Development | Your terminal | Editing code, seeing foreground logs, quick Ctrl-C restarts |
| Installed/service | `launchd` or `systemd` | Boot/login startup, crash recovery, toolbar control, real fleet behavior |

During normal development, run roles manually from the checkout:

```bash
uv run jarvis brain
uv run jarvis run
uv run jarvis worker
```

After changing Python code or `.env`, restart the manual process. This keeps
broken edits visible in the terminal and avoids `launchd` repeatedly restarting a
half-edited checkout.

Use service mode when testing installed behavior:

```bash
launchctl bootstrap gui/$UID ~/Library/LaunchAgents/com.jarvis.brain.plist
launchctl kickstart -k gui/$UID/com.jarvis.brain
launchctl print gui/$UID/com.jarvis.brain
tail -f ~/Library/Logs/Jarvis/brain.err.log
```

If you edit code while a service is running, that service keeps running the old
imported code until it is restarted:

```bash
launchctl kickstart -k gui/$UID/com.jarvis.brain
```

Do not run the same role manually and under `launchd` at the same time unless
you deliberately changed ports. The first process to bind the port wins; the
second will fail or report the port is already in use.

When you are done testing service behavior locally, unload it:

```bash
launchctl bootout gui/$UID ~/Library/LaunchAgents/com.jarvis.brain.plist
```

The Swift menu bar app targets installed/service mode. Day-to-day coding should
stay manual unless you are specifically testing the toolbar or the launchd
lifecycle.

## Homebrew Runtime Install

Mac runtime distribution should live in the Homebrew tap repository
`roughcoder/homebrew-infinite-stack`. Homebrew exposes that as the tap
`roughcoder/infinite-stack`:

```bash
curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/main/scripts/install_mac.sh | bash
```

Use `jarvis` for the runtime package: the brain, worker, intercom, CLI/runtime,
and service templates.

Reserve `jarvis-app` for the native Mac desktop/menu bar app from
`roughcoder/jarvis-apple`. That keeps the Homebrew names aligned with the split:

| Package | Owns |
|---|---|
| `jarvis-app` | Native macOS desktop/menu bar app |
| `jarvis` | Brain, workers, intercoms, CLI/runtime |

The tap can still add narrower packages later, such as `jarvis-cli`,
`jarvis-worker`, or `jarvis-agent`, if the runtime needs to split.

Mac installs use stable formula/cask releases by default. Formula HEAD installs
are reserved for runtime development and should not be used for fleet bring-up.

Homebrew should own:

- the `jarvis` command or launcher
- runtime support files
- launchd service templates
- upgradeable package metadata

Homebrew should not own:

- `.env` secrets
- pairing tokens
- device role choice
- device/user profile contents
- worker repo roots
- Docker service state

Those remain local machine configuration. An iMac, laptop, and future Mac
desktop install the same formula but enable different roles.

Example role setup after installing the formula without the app:

```bash
jarvis service sync brain worker intercom whatsapp
jarvis service install brain
jarvis service install worker
jarvis service install intercom
jarvis service install whatsapp
jarvis service start brain
jarvis fleet-status --json
```

The exact `jarvis service ...` commands are the intended interface for the
formula and app:

```bash
jarvis service install brain
jarvis service install worker
jarvis service install intercom
jarvis service start brain
jarvis service restart worker
jarvis service status intercom
jarvis service status whatsapp
```

Use `jarvis service print <role>` for dry-run inspection and CI validation.

## Status Contract

The Swift app should poll:

```bash
jarvis fleet-status --json
```

For a faster poll that skips Docker:

```bash
jarvis fleet-status --json --no-docker
```

The JSON intentionally contains no tokens. It includes:

- `services`: launchd state for `com.jarvis.brain`, `com.jarvis.intercom`,
  `com.jarvis.worker`.
- `brain`: bind address, auth configured, paired devices without secrets.
- `intercom.pairing`: whether this host can reach and pair with the configured
  brain, plus resolved identity/scope/capabilities.
- `worker.probe`: whether the configured worker is reachable, health flags, and
  recent job counts.
- `docker`: compose service states when Docker is available.
- `git`: branch, short commit, and dirty state.

The old device check remains available:

```bash
jarvis status
jarvis status --json
jarvis status --json --brain-host imac.private --brain-port 8700
```

## Mac Services

For packaged installs, prefer the app Setup window or `jarvis service ...`.
Development templates still live in `deploy/launchd/`:

- `com.jarvis.brain.plist.template`
- `com.jarvis.intercom.plist.template`
- `com.jarvis.worker.plist.template`

Create concrete plist files by replacing:

- `__JARVIS_HOME__` with this repo path, for example
  `/Users/neilbarton/Development/jarvis`.
- `__UV_BIN__` with `uv`, for example `/opt/homebrew/bin/uv`.
- `__LOG_DIR__` with a writable log directory, for example
  `/Users/neilbarton/Library/Logs/Jarvis`.

Install per user:

```bash
mkdir -p ~/Library/Logs/Jarvis
cp deploy/launchd/com.jarvis.brain.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/$UID ~/Library/LaunchAgents/com.jarvis.brain.plist
launchctl enable gui/$UID/com.jarvis.brain
launchctl kickstart -k gui/$UID/com.jarvis.brain
```

Use the matching label for `intercom` and `worker`.

Common controls:

```bash
launchctl print gui/$UID/com.jarvis.brain
launchctl kickstart -k gui/$UID/com.jarvis.brain
launchctl bootout gui/$UID ~/Library/LaunchAgents/com.jarvis.brain.plist
tail -f ~/Library/Logs/Jarvis/brain.err.log
```

## Raspberry Pi Intercom

Use the Pi installer command generated by `jarvis pair --pi-installer` or the
Setup window. It writes `/opt/jarvis`, syncs Pi-compatible intercom
dependencies, writes `/usr/local/bin/jarvis-pi`, installs the systemd unit, and
starts `jarvis-intercom.service`.

```bash
sudo jarvis-pi doctor
sudo jarvis-pi update
jarvis-pi status
jarvis-pi logs
```

The Pi remains a thin intercom: pairing token only, no provider credentials.

## Runtime Update Runbook

Update the fleet in dependency order: brain host first, linked Macs next, room
Pis last. The brain owns pairing, LiteLLM, memory, WhatsApp, and tool execution,
so linked devices should reconnect to an already-upgraded brain.

Preflight:

```bash
gh release view vX.Y.Z --repo roughcoder/jarvis
brew update
brew info roughcoder/infinite-stack/jarvis
jarvis --version
JARVIS_ENV_FILE=~/.jarvis/.env jarvis config
JARVIS_ENV_FILE=~/.jarvis/.env jarvis fleet-status --json
```

Check new envs before restarting services. For the v0.4 hardware/WebSocket
upgrade, these keys are supported and safe to omit when defaults are acceptable:

```bash
BRAIN_WEBSOCKET_MAX_SIZE=8388608
BRAIN_WEBSOCKET_PING_INTERVAL_S=20
BRAIN_WEBSOCKET_PING_TIMEOUT_S=60
INTERCOM_WEBSOCKET_MAX_SIZE=8388608
INTERCOM_WEBSOCKET_PING_INTERVAL_S=20
INTERCOM_WEBSOCKET_PING_TIMEOUT_S=60
INTERCOM_DEVICE_CAMERA=auto
INTERCOM_DEVICE_CAMERA_BIN=
INTERCOM_DEVICE_CAMERA_WIDTH=1280
INTERCOM_DEVICE_CAMERA_HEIGHT=720
INTERCOM_DEVICE_CAMERA_TIMEOUT_S=8
INTERCOM_DEVICE_CAMERA_WARMUP_MS=300
INTERCOM_DEVICE_PI_PANEL=auto
INTERCOM_DEVICE_PI_PANEL_SLEEP_AFTER_S=90
# Pin PiPanel to the 4.3-inch Pironman DSI panel if HDMI is also active.
INTERCOM_DEVICE_PI_PANEL_GEOMETRY=800x480+0+0
```

Brain Mac:

```bash
brew upgrade roughcoder/infinite-stack/jarvis
JARVIS_ENV_FILE=~/.jarvis/.env jarvis service sync brain worker whatsapp
JARVIS_ENV_FILE=~/.jarvis/.env jarvis service restart brain
sleep 8
JARVIS_ENV_FILE=~/.jarvis/.env jarvis service restart worker
JARVIS_ENV_FILE=~/.jarvis/.env jarvis service restart whatsapp
JARVIS_ENV_FILE=~/.jarvis/.env jarvis ping-gateway --route fast
JARVIS_ENV_FILE=~/.jarvis/.env jarvis ping-gateway --route strong
JARVIS_ENV_FILE=~/.jarvis/.env jarvis fleet-status --json
```

If the Homebrew wrapper reports recursive `uv run`, repair the installed venv
and rerun the version check:

```bash
cd /opt/homebrew/Cellar/jarvis/X.Y.Z/libexec
/opt/homebrew/opt/uv/bin/uv sync --no-dev
jarvis --version
```

If launchd gets stuck after a brain restart, recover cleanly instead of stacking
restarts:

```bash
uid=$(id -u)
plist="$HOME/Library/LaunchAgents/com.jarvis.brain.plist"
launchctl bootout "gui/$uid" "$plist" 2>/dev/null || true
lsof -ti tcp:8700 | xargs kill 2>/dev/null || true
launchctl bootstrap "gui/$uid" "$plist"
launchctl kickstart -k "gui/$uid/com.jarvis.brain"
launchctl print "gui/$uid/com.jarvis.brain"
lsof -nP -iTCP:8700 -sTCP:LISTEN
```

Linked Macs:

```bash
brew update
brew upgrade roughcoder/infinite-stack/jarvis
JARVIS_ENV_FILE=~/.jarvis/.env jarvis service sync worker intercom
JARVIS_ENV_FILE=~/.jarvis/.env jarvis service restart worker
sleep 3
JARVIS_ENV_FILE=~/.jarvis/.env jarvis service restart intercom
JARVIS_ENV_FILE=~/.jarvis/.env jarvis fleet-status --json --no-docker
```

Room Pis:

```bash
ssh alice@<tailscale-ip> 'sudo jarvis-pi update && jarvis-pi status && jarvis-pi doctor'
```

If a Pi is offline in Tailscale, do not continue the Pi step from the brain Mac.
Ask for a physical power/network check, then retry SSH when Tailscale reports the
host online.

Pi screens are optional hardware. The SunFounder Pironman 5 Pro Max screen is a
4.3-inch 800x480 MIPI DSI touch display. Keep `INTERCOM_DEVICE_PI_PANEL=auto`
until the systemd service has a real display session (`DISPLAY` or
`WAYLAND_DISPLAY`); forcing PiPanel on a headless unit can make the UI probe
fail. If HDMI is also active, set
`INTERCOM_DEVICE_PI_PANEL_GEOMETRY=800x480+0+0` so the panel does not size itself
against the combined virtual desktop. Grant `intercom.display` only on profiles
for Pis that actually have a working screen. Existing `INTERCOM_DEVICE_EYES`
values remain supported as legacy aliases.

Post-update smoke:

```bash
JARVIS_ENV_FILE=~/.jarvis/.env jarvis text --once "Smoke test after upgrade: reply with only OK."
JARVIS_ENV_FILE=~/.jarvis/.env jarvis traces -n 10
```

In LiteLLM spend logs, Jarvis turns should be filterable by request tags:
`kind:turn`, `channel:text` or `channel:whatsapp`, `speaker:<person>`, and
`device:<device_id>`. Heartbeats should show `end_user=heartbeat` and tags
`kind:heartbeat`, `channel:system`, `speaker:heartbeat`.

On newer Raspberry Pi OS / Debian releases, the system Python may be newer than
Jarvis supports. Pin the installer to a compatible managed Python when needed:

```bash
sudo uv python install 3.11
sudo JARVIS_PYTHON_BIN=3.11 ... bash /tmp/install_jarvis_pi.sh
```

Some USB microphones only expose 44.1/48 kHz in hardware while Jarvis captures
16 kHz for wake/VAD/STT. Use ALSA's `plug` layer for capture resampling rather
than binding Jarvis directly to the raw `hw:*` device. A minimal Pi
`/etc/asound.conf` shape is:

```conf
pcm.!default {
    type asym
    playback.pcm "plughw:CARD=vc4hdmi0,DEV=0"
    capture.pcm "plughw:CARD=Device,DEV=0"
}

ctl.!default {
    type hw
    card Device
}
```

When the brain is reached over Tailscale, harden the Pi systemd unit so Jarvis
does not start before the tailnet route to the brain exists. The important parts
are:

```ini
[Unit]
After=network-online.target tailscaled.service sound.target
Wants=network-online.target tailscaled.service

[Service]
ExecStartPre=/bin/sh -c 'for i in $(seq 1 120); do /usr/bin/nc -z -w 2 <brain-tailscale-ip> 8700 && exit 0; sleep 2; done; echo "Jarvis brain not reachable" >&2; exit 1'
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1
```

Verify after a reboot with:

```bash
sudo systemctl status jarvis-intercom.service
sudo journalctl -u jarvis-intercom.service -b --no-pager
ss -tanp | grep ':8700'
```

The healthy log line is:

```text
Paired with brain.
Jarvis is listening.
```

## WhatsApp Connector

The WhatsApp connector is a boundary service around `wacli`. It runs on the
brain host or another Mac that can reach the brain WebSocket. It holds only a
brain pairing token and the local `wacli` linked-device state; provider keys stay
on the brain/gateway side.

Install and authenticate `wacli` first:

```bash
brew tap openclaw/tap
brew trust --formula openclaw/tap/wacli
brew install wacli
wacli auth --qr-format terminal
```

Scan the QR from WhatsApp's **Linked devices** screen. Then enable the Jarvis
role:

```bash
jarvis pair whatsapp --apply-brain-config --env-file "$HOME/.jarvis/.env" --brain-bind-host 0.0.0.0 --json
jarvis service sync whatsapp
jarvis service install whatsapp --workdir "$HOME/.jarvis" --jarvis-bin /opt/homebrew/bin/jarvis
jarvis service start whatsapp
```

Recommended access policy:

```dotenv
WHATSAPP_ENABLED=true
WHATSAPP_DEVICE_ID=whatsapp
WHATSAPP_DM_POLICY=pairing
WHATSAPP_ADMIN=<admin-number-digits>
WHATSAPP_GROUP_POLICY=ignore
WHATSAPP_TRIGGER=jarvis
```

With `WHATSAPP_DM_POLICY=pairing`, numbers already listed in
`jarvis-workspace/users/*.md` under `whatsapp: [...]` can message Jarvis
directly. Unknown senders receive a holding reply and the admin receives an
approval command.

Useful checks:

```bash
wacli doctor
jarvis service status whatsapp
tail -f "$HOME/Library/Logs/Jarvis/whatsapp.out.log" "$HOME/Library/Logs/Jarvis/whatsapp.err.log"
lsof -nP -iTCP:8700
```

`wacli doctor` may report `locked_by_other_process` while the Jarvis service is
running because `wacli sync --follow` owns the store lock. That is expected.

## Network Binding

For an iMac brain reachable by other devices:

```bash
BRAIN_HOST=0.0.0.0
BRAIN_DEVICES=[{"token":"mac-secret","device_id":"neil-mac","identity":"neil"},{"token":"pi-secret","device_id":"room-pi"}]
```

For a laptop worker reachable by the brain:

```bash
WORKER_HOST=<laptop-private-network-name>
WORKER_BIND_HOST=0.0.0.0
WORKER_TOKEN=<long-random-secret>
```

The brain and worker refuse non-loopback no-token binds unless the matching
`*_ALLOW_INSECURE=true` override is set. Do not use that override for the fleet.

## Update Shape

The installed update path should stay boring:

```bash
brew update
brew upgrade jarvis
jarvis service sync brain worker intercom whatsapp
jarvis service restart brain
jarvis service restart worker
jarvis service restart intercom
jarvis service restart whatsapp
```

The app Setup window's **Update Runtime** action performs the same role-scoped
sync/restart flow for the roles selected on the current Mac.

Pi hosts use:

```bash
sudo jarvis-pi update
```

Development checkouts can still use:

```bash
git pull --ff-only
uv sync --extra gateway --extra tts --extra stt --extra vad --extra wake --extra memory
launchctl kickstart -k gui/$UID/com.jarvis.brain
```

The Swift app should run the role-appropriate sequence only for roles installed
on the current machine, then re-poll `jarvis fleet-status --json`.
