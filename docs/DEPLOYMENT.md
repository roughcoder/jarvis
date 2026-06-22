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
| iMac | `brain`, `worker`, optional local `intercom`, Docker services | `brew install jarvis` + `brew install --cask jarvis-app` | `launchd` |
| Mac laptop | `intercom`, `worker` | `brew install jarvis` + `brew install --cask jarvis-app` | `launchd` |
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
The current runtime formula is `--HEAD` while versioned runtime tarballs are
prepared.

## Stable CLI Surface

Deployment tools should call these commands instead of editing service files by
hand:

```bash
jarvis service install brain
jarvis service install worker
jarvis service install intercom
jarvis service start brain
jarvis service restart worker
jarvis service status intercom
jarvis service extras brain worker
jarvis pair kitchen-pi --json
```

`jarvis service print ...` is the dry, inspectable form used by installers and
tests. On macOS it renders `launchd` plists; on Linux it renders `systemd` units.

## Onboarding Flow

The native app should drive setup in five stages:

1. **Role choice**: Brain Mac, Laptop, Worker-only Mac, Room Pi helper.
2. **Prerequisites**: Homebrew, Twingate reachability, microphone permission,
   speaker output, Docker for brain hosts, Chrome/GUI permission for workers.
3. **Pairing**: Device requests approval from the brain or the app generates a
   per-device token entry with `jarvis pair`.
4. **Service install**: App calls `jarvis service install/start` for the selected
   roles and shows logs on failure.
5. **Ready state**: App polls `jarvis fleet-status --json` until every selected
   role is green or gives one concrete fix.

The app should treat development checkouts and packaged installs differently:

- Dev checkout: update may use git dirty checks and `git pull --ff-only`.
- Brew install: update should use `brew upgrade jarvis` and
  `brew upgrade --cask jarvis-app`.

## Pi Provisioning

The first Pi path should be a script installer; once the hardware stack is stable,
ship a prebuilt image.

Target flow:

```bash
curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/main/scripts/install_pi.sh \
  -o /tmp/install_jarvis_pi.sh
sudo JARVIS_BRAIN_HOST=imac.private \
  JARVIS_INTERCOM_TOKEN=issued-token \
  JARVIS_DEVICE_ID=kitchen-pi \
  bash /tmp/install_jarvis_pi.sh
```

The Pi installer should:

- install OS packages for audio, Python runtime support, and systemd service use
- install the `jarvis` intercom runtime
- detect microphone, speaker, and optional camera
- ask for the brain hostname or discover it on the Twingate/private network
- show a pairing code on the screen
- install `jarvis-intercom.service`
- start the service only after pairing succeeds

The Pi remains a thin intercom: no LLM keys, no memory keys, no worker/browser
control, and no personal user files.

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
brew upgrade --fetch-HEAD jarvis
jarvis service restart brain
jarvis service restart worker
jarvis service restart intercom
```

Mac app:

```bash
brew upgrade --cask jarvis-app
```

Pi runtime:

```bash
jarvis-pi update
systemctl restart jarvis-intercom
```

Until the Pi package exists, the installer should provide an update command that
refreshes the installed runtime and restarts the service.

## Acceptance Gates

- A clean Mac can install runtime + app without cloning a repo.
- A clean Pi can become an intercom without editing `.env` by hand.
- The app can install, start, stop, update, and diagnose selected local roles.
- A new device cannot join the fleet without brain approval or a generated
  per-device token entry.
- Public repositories contain no personal user files, OAuth caches, browser
  profiles, worker job records, real tokens, or private release-only assumptions.
