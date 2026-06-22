# Jarvis Fleet Deployment

Jarvis runs as a set of local roles. A host may run one role or several roles, but
each role stays independently supervised:

| Host | Roles |
|---|---|
| Mac mini | `brain`, Docker services, optional `intercom`, optional `worker` |
| Mac laptop | `intercom`, `worker` |
| Raspberry Pi | `intercom` |

`launchd` or `systemd` owns the long-running processes. Operator UIs, including
the planned Swift menu bar app, only observe status and request safe actions.

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

Use `jarvis` for the runtime package: the brain, worker, intercom, CLI/runtime
bootstrapper, and service templates.

Reserve `jarvis-app` for the native Mac desktop/menu bar app from
`roughcoder/jarvis-apple`. That keeps the Homebrew names aligned with the split:

| Package | Owns |
|---|---|
| `jarvis-app` | Native macOS desktop/menu bar app |
| `jarvis` | Brain, workers, intercoms, CLI/runtime bootstrapper |

The tap can still add narrower packages later, such as `jarvis-cli`,
`jarvis-worker`, or `jarvis-agent`, if the runtime needs to split.

The public Mac bootstrap installs stable formula/cask releases by default.
Formula HEAD fallback is reserved for development and must be explicitly enabled
with `JARVIS_ALLOW_HEAD_FALLBACK=1`.

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

Those remain local machine configuration. A Mac mini, laptop, and future Mac
desktop install the same formula but enable different roles.

Example role setup after installing the formula:

```bash
jarvis service install brain
jarvis service install worker
jarvis service install intercom
jarvis service start brain
jarvis fleet-status --json
```

The exact `jarvis service ...` commands are the intended interface for the
formula/bootstrapper. Until they exist, use the launchd templates below directly.

The service command surface now exists for packaged installers:

```bash
jarvis service install brain
jarvis service install worker
jarvis service install intercom
jarvis service start brain
jarvis service restart worker
jarvis service status intercom
```

Use `jarvis service print <role>` for dry-run inspection and CI validation.

## Status Contract

The Swift app should poll:

```bash
uv run jarvis fleet-status --json
```

For a faster poll that skips Docker:

```bash
uv run jarvis fleet-status --json --no-docker
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
uv run jarvis status
uv run jarvis status --json
```

## Mac Services

Templates live in `deploy/launchd/`:

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

Template:

- `deploy/systemd/jarvis-intercom.service.template`

Replace `__JARVIS_HOME__` and `__UV_BIN__`, then install:

```bash
sudo cp deploy/systemd/jarvis-intercom.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now jarvis-intercom
systemctl status jarvis-intercom
journalctl -u jarvis-intercom -f
```

The Pi remains a thin intercom: pairing token only, no provider credentials.

## Network Binding

For a Mac mini brain reachable by other devices:

```bash
BRAIN_HOST=0.0.0.0
BRAIN_DEVICES=[{"token":"mac-secret","device_id":"neil-mac","identity":"neil"},{"token":"pi-secret","device_id":"room-pi"}]
```

For a laptop worker reachable by the brain:

```bash
WORKER_HOST=<laptop-tailscale-name>
WORKER_BIND_HOST=0.0.0.0
WORKER_TOKEN=<long-random-secret>
```

The brain and worker refuse non-loopback no-token binds unless the matching
`*_ALLOW_INSECURE=true` override is set. Do not use that override for the fleet.

## Update Shape

The update path should stay boring:

```bash
git pull --ff-only
uv sync --extra gateway --extra tts --extra stt --extra vad --extra wake --extra memory
launchctl kickstart -k gui/$UID/com.jarvis.brain
```

Worker hosts usually need:

```bash
git pull --ff-only
uv sync --extra worker --extra browser
launchctl kickstart -k gui/$UID/com.jarvis.worker
```

Intercom hosts usually need:

```bash
git pull --ff-only
uv sync --extra stt --extra vad --extra wake
launchctl kickstart -k gui/$UID/com.jarvis.intercom
```

The Swift app should run the role-appropriate sequence only for roles installed
on the current machine, then re-poll `jarvis fleet-status --json`.
