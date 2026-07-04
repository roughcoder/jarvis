# Jarvis Menu Bar App Spec

This is the handoff spec for a separate Swift repository. The app is an operator
UI for Jarvis roles installed on the current Mac. It must not embed Jarvis brain,
intercom, worker, memory, gateway, or capability logic.

## Product

Name: Jarvis Menu Bar

Runtime: native macOS menu bar app, Swift/SwiftUI, using `MenuBarExtra`.

Primary job: show what this Mac is running, show whether the rest of the Jarvis
fleet is reachable, and provide safe local controls.

## Ownership Model

Long-running services are owned by `launchd`:

- `com.jarvis.brain`
- `com.jarvis.api`
- `com.jarvis.intercom`
- `com.jarvis.worker`

The menu bar app observes and controls those services. It is not the supervisor
and Jarvis must keep running if the app quits.

The app targets installed/service mode. During active Jarvis development, roles
are usually run manually in terminals from the same checkout; the app should
handle "service not loaded" as a normal state, not as an installation failure.

## Data Sources

The app's primary status source is:

```bash
uv run jarvis fleet-status --json --no-docker
```

Use full Docker status on slower refreshes:

```bash
uv run jarvis fleet-status --json
```

The app may call:

```bash
uv run jarvis status --json
uv run jarvis config
uv run jarvis worker --doctor
uv run jarvis jobs -n 10
uv run jarvis traces -n 10
```

All commands should run from the Jarvis repo root. The Jarvis repo path, `uv`
path, and log directory should be user-configurable in app settings.

## Status Model

Parse the JSON returned by `fleet-status`. The top-level fields currently are:

- `version`
- `device_id`
- `platform`
- `services`
- `brain`
- `intercom`
- `worker`
- `docker`
- `git`

Status badges:

| Role | Green | Amber | Red |
|---|---|---|---|
| Brain | launchd loaded and intercom pairing reachable | loaded but auth not configured, or Docker degraded | stopped or unreachable |
| Intercom | service loaded and paired with brain | service loaded but unpaired | stopped |
| Worker | service loaded and `/health` reachable | reachable but jobs errored or GUI/browser not configured | stopped or unreachable |
| Docker | all required compose services running | Docker unavailable on a non-brain host, or optional services stopped | required brain services stopped |
| Update | clean git state at expected branch | dirty tree | update failed |

The app should tolerate missing fields and show "unknown" rather than crashing.

## Local Controls

Controls are local-only in v1. A laptop app can show remote brain reachability,
but it must not SSH into the Mac mini or restart remote services.

Use `launchctl`:

```bash
launchctl print gui/$UID/com.jarvis.brain
launchctl kickstart -k gui/$UID/com.jarvis.brain
launchctl bootout gui/$UID ~/Library/LaunchAgents/com.jarvis.brain.plist
launchctl bootstrap gui/$UID ~/Library/LaunchAgents/com.jarvis.brain.plist
```

Equivalent labels exist for `intercom` and `worker`.

Use Docker only on hosts with the Docker stack:

```bash
docker compose up -d
docker compose restart
docker compose ps
```

Use logs:

```bash
tail -n 200 ~/Library/Logs/Jarvis/brain.err.log
tail -n 200 ~/Library/Logs/Jarvis/intercom.err.log
tail -n 200 ~/Library/Logs/Jarvis/worker.err.log
```

## Update Flow

The app should present update as an explicit command, not automatic background
mutation.

Role-specific update:

1. Check `git.dirty`. If dirty, show a blocking warning with the changed state.
2. Run `git pull --ff-only`.
3. Run the role-specific `uv sync` extras.
4. Restart only the installed roles on this Mac.
5. Re-poll `fleet-status --json`.
6. Show the last command output if any step fails.

Suggested extras:

| Installed role | Sync command |
|---|---|
| Brain | `uv sync --extra gateway --extra tts --extra stt --extra vad --extra wake --extra memory --extra mcp` |
| Worker | `uv sync --extra worker --extra browser` |
| Intercom | `uv sync --extra stt --extra vad --extra wake` |

If multiple roles are installed, union the extras and run one `uv sync`.

## UI

Menu content:

- Overall status dot: green, amber, red.
- Current device id and git revision.
- Role rows for Brain, Intercom, Worker, Docker.
- Connected/pairing summary: identity, scope, capability count.
- Worker summary: running jobs, recent job statuses.
- Buttons: Start, Restart, Stop, Logs, Update.
- Quick actions: Open Jarvis repo, Open logs folder, Copy status JSON.

Settings window:

- Jarvis repo path.
- `uv` binary path.
- Logs path.
- Installed roles on this Mac.
- Poll interval.
- Whether Docker checks are enabled.

## Security

- Do not display pairing tokens, worker tokens, API keys, or `.env` values.
- Do not edit `.env` in v1.
- Do not run destructive git commands.
- Do not auto-update while `git.dirty=true`.
- Do not expose remote restart controls until Jarvis has an authenticated admin
  API. v1 is local-only control.

## Polling

Default poll:

- Every five seconds: `fleet-status --json --no-docker`.
- Every thirty seconds: full `fleet-status --json`.
- Immediately after every command: full status refresh.

Command execution should have timeouts and cancellation. Long-running update
steps need a progress sheet with stdout/stderr capture.

## Non-Goals

- No Python embedding.
- No direct parsing of `.env`.
- No direct WebSocket protocol implementation in v1.
- No remote SSH control.
- No replacement for launchd/systemd.
