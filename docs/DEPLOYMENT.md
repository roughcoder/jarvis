# Jarvis Deployment

Jarvis deployment should feel like installing an appliance, not setting up a
developer checkout. Development can still use `uv` and foreground terminals; fleet
machines should use packaged installers, stable service commands, and the native
Jarvis app.

Frankfurt/Coolify is intentionally out of scope for this phase. The deployment
shape below covers the iMac brain, Mac workers/intercoms, Raspberry Pi intercoms,
Homebrew packaging, onboarding, pairing, and updates.

## Target Fleet

| Machine | Roles | Package path | Supervisor |
|---|---|---|---|
| iMac | `brain`, `api`, `worker`, optional local `intercom`, Docker services | Homebrew + Jarvis Setup | `launchd` |
| Mac laptop | `intercom`, `worker` | Homebrew + Jarvis Setup | `launchd` |
| Raspberry Pi | `intercom` | Pi installer / future image | `systemd` |

The iMac brain is the local authority for device pairing. Each device has its own
device id and token. Tokens are bound to device ids, so a leaked Pi token cannot
impersonate a Mac.

## Package Split

| Package | Owns | Does not own |
|---|---|---|
| `jarvis` formula | Runtime, CLI, service manager, launchd/systemd file rendering, role dependency sync, Pi-compatible intercom install path | Provider secrets, user profiles, pairing decisions |
| `jarvis-app` cask | Native macOS operator/onboarding app | Brain logic, worker logic, memory, provider SDKs |
| Pi installer | Linux dependencies, audio/display checks, `jarvis` intercom runtime, systemd unit | Brain, worker, provider credentials |

The final user path should avoid clone instructions. `uv` may remain an internal
implementation detail for the formula while the public surface stays `jarvis`.
The runtime now publishes versioned tarballs and the tap formula has a stable
release URL; `--HEAD` remains only as a development fallback.

## Stable CLI Surface

Deployment tools should call these commands instead of editing service files by
hand:

```bash
jarvis service install brain
jarvis service install api
jarvis service install worker
jarvis service install intercom
jarvis service start brain
jarvis service start api
jarvis service restart worker
jarvis service status intercom
jarvis service extras brain api worker
jarvis status --json --brain-host imac.private
jarvis pair kitchen-pi --json
```

`jarvis service print ...` is the dry, inspectable form used by installers and
tests. On macOS it renders `launchd` plists; on Linux it renders `systemd` units.

## Mac Install

The public Mac install surface is one command:

```bash
curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/main/scripts/install_mac.sh | bash
```

The installer uses Homebrew internally, prepares `~/.jarvis`, clears app
quarantine while Jarvis is ad-hoc signed, and opens Jarvis. The Setup window
then owns role choice, local service installation, and pairing.

Packaged Mac services installed from the app use `~/.jarvis` as
the service workdir and set `JARVIS_ENV_FILE=~/.jarvis/.env` in launchd. This
keeps local pairing/provider configuration outside the Homebrew Cellar and
independent of the app's current directory.

Clean uninstall for fresh end-to-end testing removes launchd services, local
state, logs, app preferences, the Homebrew cask, and the Homebrew runtime. It
does not touch local source checkouts:

```bash
curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/main/scripts/uninstall_mac.sh | bash
```

## Onboarding Flow

The native app should drive setup in five stages:

1. **Role choice**: Brain Mac, Laptop, Worker-only Mac, Room Pi helper.
2. **Prerequisites**: Homebrew, Twingate reachability, microphone permission,
   speaker output, Docker for brain hosts, Chrome/GUI permission for workers.
3. **Pairing**: Device checks the configured brain host with
   `jarvis status --json --brain-host ...`, then requests approval from the
   brain or the app generates a per-device token entry with `jarvis pair`.
   Pairing output can include a Mac config command for laptops/workers and a Pi
   installer command for room intercoms.
4. **Service install**: App calls `jarvis service install/start` for the selected
   roles and shows logs on failure.
5. **Ready state**: App polls `jarvis fleet-status --json` until every selected
   role is green or gives one concrete fix.

Current fresh-fleet sequence:

1. On the iMac, install with Homebrew and choose **Brain Mac** in Setup.
2. Install/start the selected services from Setup. When **Brain Mac** roles are
   selected, Setup writes `BRAIN_HOST=0.0.0.0` and issued `BRAIN_DEVICES`
   entries into `~/.jarvis/.env`.
3. On each laptop, install with Homebrew and choose **Laptop** in Setup.
   Use the iMac Setup window's **Issue Token** action and copy the Mac config
   command onto the laptop before installing services.
4. For each room Pi, issue a token from the iMac Setup window and run the copied
   Pi installer command on the Pi. The command is pinned to the current runtime
   release tag by default.
5. Use Setup's **Check Brain**, **Check Worker**, and status polling after each
   device joins the Twingate/private network.

The app should treat development checkouts and packaged installs differently:

- Dev checkout: update may use git dirty checks and `git pull --ff-only`.
- Brew install: update should use `brew upgrade jarvis` and
  `brew upgrade --cask jarvis-app`.

## Pi Provisioning

The first Pi path should be a script installer; once the hardware stack is stable,
ship a prebuilt image.

Target flow:

```bash
JARVIS_REF=v0.1.21
curl -fsSL "https://raw.githubusercontent.com/roughcoder/jarvis/$JARVIS_REF/scripts/install_pi.sh" \
  -o /tmp/install_jarvis_pi.sh
sudo JARVIS_BRAIN_HOST=imac.private \
  JARVIS_INTERCOM_TOKEN=issued-token \
  JARVIS_DEVICE_ID=kitchen-pi \
  JARVIS_REF="$JARVIS_REF" \
  bash /tmp/install_jarvis_pi.sh
```

The Pi installer should:

- install OS packages for audio, Python runtime support, and systemd service use
- install the `jarvis` intercom runtime
- use Raspberry Pi OS Python 3.11 plus the lightweight `vad-lite` WebRTC backend
  instead of the Mac-only Silero/PyTorch VAD stack
- install a `jarvis-pi` helper for update, restart, status, logs, and hardware
  readiness checks
- detect microphone, speaker, and optional camera with `jarvis-pi doctor`
- receive the brain hostname and token from the generated installer command
- install `jarvis-intercom.service`
- start the service after the token-bound local config has been written

Pi installer commands generated by `jarvis pair --pi-installer` are pinned to
the current runtime release tag by default, so a fresh Pi installs the same
released code as the brain that issued the token. Use `--ref main` only for
development or hardware bring-up against unreleased runtime code.

Preview the command sequence without root or hardware changes:

```bash
JARVIS_DRY_RUN=1 \
  JARVIS_BRAIN_HOST=imac.private \
  JARVIS_INTERCOM_TOKEN=issued-token \
  JARVIS_DEVICE_ID=kitchen-pi \
  bash scripts/install_pi.sh
```

The Pi remains a thin intercom: no LLM keys, no memory keys, no worker/browser
control, and no personal user files.

## Mac Pairing Config

For Mac laptops and worker Macs, the brain can issue one command that writes the
local intercom pairing config into the same `~/.jarvis/.env` file used by
packaged services:

```bash
jarvis pair neil-laptop --mac-config --brain-host imac.private --identity neil
```

The command can write `BRAIN_HOST` plus a `BRAIN_DEVICES` entry to the brain env
file with `--apply-brain-config --brain-bind-host 0.0.0.0`. It also prints a
copy/paste shell snippet for the target Mac. The snippet upserts only the Jarvis
pairing keys:
`INTERCOM_BRAIN_HOST`, `INTERCOM_BRAIN_PORT`, `INTERCOM_TOKEN`,
`CAPS_DEVICE_ID`, `CAPS_IDENTITY`, and `CAPS_SCOPE`.

## Network Model

Every device reaches the iMac brain over its Twingate/private-network name. The
app should check:

- brain WebSocket reachability
- worker HTTP reachability when a worker is installed
- per-device pairing result
- no non-loopback unauthenticated brain or worker binds

No remote restart or shell control should exist until Jarvis has an authenticated
admin API. Local app controls remain local-only.

## Update Model

Updates should be boring and explicit.

Mac runtime:

```bash
brew update
brew upgrade jarvis
jarvis service restart brain
jarvis service restart worker
jarvis service restart intercom
```

Mac app:

```bash
brew upgrade --cask jarvis-app
```

The Setup window's **Update Runtime** action performs the runtime update for
Homebrew-managed installs, syncs the selected role extras, restarts the selected
launchd services, and refreshes fleet status.

Pi runtime:

```bash
sudo jarvis-pi update
```

The Pi installer writes `/usr/local/bin/jarvis-pi`; `update` refreshes the
installed runtime, syncs intercom dependencies, reloads systemd, and restarts
`jarvis-intercom.service`. `jarvis-pi doctor` prints basic service, audio,
display, and camera readiness.

## Acceptance Gates

- A clean Mac can install runtime + app without cloning a repo.
- A clean Pi can become an intercom without editing `.env` by hand.
- The app can install, start, stop, update, and diagnose selected local roles.
- A new device cannot join the fleet without brain approval or a generated
  per-device token entry.
- `jarvis bringup --json` can collect redacted install, package, service,
  hardware, and pairing evidence on each physical device.
- Public repositories contain no personal user files, OAuth caches, browser
  profiles, worker job records, real tokens, or private release-only assumptions.
