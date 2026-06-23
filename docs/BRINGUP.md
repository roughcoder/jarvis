# Fleet Bring-Up Checklist

Use this checklist when the physical machines are available. It records the
evidence needed to prove the fresh install, pairing, service, update, and
hardware paths across the fleet.

GitHub Pages is deliberately out of scope for the current gate. The prepared
`docs-site/` preview must stay accurate, but hosting is deferred.

## Evidence Folder

Create one local evidence folder per bring-up session:

```bash
mkdir -p ~/Desktop/jarvis-bringup-evidence
```

Use `jarvis bringup --output ~/Desktop/jarvis-bringup-evidence` to save a
timestamped redacted JSON proof file on each device. Save any extra command
output with the machine name in each filename, for example `imac-install.txt`,
`laptop-neil-worker.txt`, and `kitchen-pi-doctor.txt`. Do not commit these
files; they may contain hostnames or local machine details.

## iMac Brain

Install:

```bash
curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/main/scripts/install_mac.sh | bash
```

Configure `~/.jarvis/.env` with:

```bash
BRAIN_HOST=0.0.0.0
BRAIN_DEVICES=[{"token":"issued-token","device_id":"example-device"}]
```

Use Jarvis Setup:

1. Choose **Brain Mac**.
2. Set the private-network brain hostname, for example `imac.private`.
3. Install services.
4. Issue tokens for each laptop and Pi.

Proof:

```bash
jarvis bringup --json --role brain --role worker --role intercom --hardware \
  --output ~/Desktop/jarvis-bringup-evidence
jarvis --version
brew list --formula --versions jarvis
brew list --cask --versions jarvis-app
jarvis service status brain
jarvis service status worker
jarvis service status intercom
jarvis fleet-status --json
```

Pass criteria:

- `jarvis --version` matches the Homebrew formula version.
- `jarvis-app` is installed through the cask.
- Selected launchd services are loaded.
- `fleet-status --json` contains no token values.
- Brain bind is intentional: public/private-network reachability uses
  `BRAIN_HOST=0.0.0.0` plus configured `BRAIN_DEVICES`, not an insecure no-token
  bind.

## Mac Laptops

Install:

```bash
curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/main/scripts/install_mac.sh | bash
```

On the iMac, issue a laptop token from Setup or:

```bash
jarvis pair neil-laptop --mac-config --brain-host imac.private --identity neil
```

Run the generated Mac config command on the laptop, then use Jarvis Setup:

1. Choose **Laptop**.
2. Confirm the brain hostname is set.
3. Check brain reachability.
4. Check worker readiness.
5. Install services.

Proof:

```bash
jarvis bringup --json --role intercom --role worker --hardware \
  --brain-host imac.private --output ~/Desktop/jarvis-bringup-evidence
jarvis --version
jarvis status --json --brain-host imac.private
jarvis worker --doctor
jarvis service status intercom
jarvis service status worker
jarvis fleet-status --json --no-docker
```

Pass criteria:

- `status --json --brain-host ...` is reachable and paired.
- `worker --doctor` reports the GUI dependency path and any remaining macOS
  Screen Recording or Accessibility permission steps.
- Intercom and worker launchd services are loaded for the selected profile.
- The laptop can be updated without a repo clone:

  ```bash
  brew update
  brew upgrade jarvis
  jarvis service sync intercom worker
  brew upgrade --cask jarvis-app
  ```

## Raspberry Pi Room Intercoms

On the iMac, issue a Pi token from Setup or:

```bash
jarvis pair kitchen-pi --json --pi-installer --brain-host imac.private
```

Run the generated Pi installer command on the Pi. It should include a release tag
such as `JARVIS_REF=v0.1.10`, not a development-only `main` ref.

Proof:

```bash
jarvis bringup --json --role intercom --platform systemd --hardware \
  --brain-host imac.private --output ~/Desktop/jarvis-bringup-evidence
jarvis-pi doctor
jarvis-pi status
systemctl is-enabled jarvis-intercom.service
systemctl is-active jarvis-intercom.service
journalctl -u jarvis-intercom.service -n 80 --no-pager
```

Pass criteria:

- `jarvis bringup --json --output ...` is valid JSON, saves a timestamped
  evidence file, and contains no raw token values.
- `jarvis-pi doctor` shows the expected `CAPS_DEVICE_ID`,
  `INTERCOM_BRAIN_HOST`, and `INTERCOM_BRAIN_PORT`.
- `arecord -l` and `aplay -l` list the expected microphone and speaker.
- `libcamera-hello --list-cameras` lists the camera when the camera package is
  installed and hardware is attached.
- `jarvis-intercom.service` is enabled and active after the matching
  `BRAIN_DEVICES` entry is present on the iMac.
- Pi updates are one command:

  ```bash
  sudo jarvis-pi update
  ```

## Private Network

From every laptop and Pi:

```bash
nc -vz imac.private 8700
jarvis status --json --brain-host imac.private
```

From the iMac, verify worker hosts only if worker APIs are intended to be
reachable from the brain:

```bash
jarvis fleet-status --json
```

Pass criteria:

- Every intercom device reaches the iMac brain over the private-network name.
- No worker or brain service is bound to a non-loopback address without a token.
- Pairing failures are explicit and fixable by checking `INTERCOM_TOKEN`,
  `CAPS_DEVICE_ID`, and `BRAIN_DEVICES`.

## Completion Gate

Deployment readiness is physically proven only after:

- one clean iMac install completes through bootstrap and Setup
- two clean laptop installs complete through bootstrap and Setup
- one clean Pi install completes from a generated release-pinned command
- all selected services survive a restart
- all device pairing checks pass over the private network
- one runtime update succeeds on a Mac through Homebrew
- one app update succeeds through the cask or in-app Homebrew updater
- one Pi update succeeds through `sudo jarvis-pi update`
