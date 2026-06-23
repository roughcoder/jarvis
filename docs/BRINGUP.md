# Fleet Bring-Up Checklist

Use this checklist when the physical machines are available. It records the
evidence needed to prove the fresh install, pairing, service, update, and
hardware paths across the fleet.

The static deployment preview lives in `docs-site/` and is published by the
GitHub Pages workflow from `main`. Keep the preview aligned with this checklist
whenever install, pairing, update, or evidence commands change.

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

After copying all `jarvis-bringup-*.json` files into the session folder, run a
single summary gate:

```bash
jarvis bringup-summary ~/Desktop/jarvis-bringup-evidence \
  --expect-role brain --expect-role worker --expect-role intercom \
  --expect-current-release --min-files 4 \
  --output ~/Desktop/jarvis-bringup-evidence/jarvis-fleet-summary.json
```

This writes the fleet summary JSON into the evidence folder. Add `--json` if you
want the same summary printed to stdout for automation.

## Fresh Fleet Runbook

Use this order for the first clean fleet: one iMac brain, two Mac laptops, and
one room Pi. Replace `imac.private`, laptop ids, and Pi ids with the names used
on the private network.

1. **iMac first**

   ```bash
   brew tap roughcoder/infinite-stack
   brew trust --formula roughcoder/infinite-stack/jarvis
   brew trust --cask roughcoder/infinite-stack/jarvis-app
   brew install jarvis
   brew install --cask jarvis-app
   /usr/bin/xattr -dr com.apple.quarantine /Applications/Jarvis.app
   open -a Jarvis
   ```

   For a totally fresh Mac, install Homebrew first using the official Homebrew installer, then run the Homebrew commands above.

   In Jarvis Setup, choose **Brain Mac**, set the private-network host
   (`imac.private`), install services, and keep Setup open for token issuing.
   Confirm:

   ```bash
   jarvis --version
   brew list --formula --versions jarvis
   brew list --cask --versions jarvis-app
   jarvis fleet-status --json
   ```

2. **Issue both laptop pairings from the iMac**

   ```bash
   jarvis pair neil-laptop --mac-config --brain-host imac.private --identity neil
   jarvis pair second-laptop --mac-config --brain-host imac.private --identity neil
   ```

   Use the generated Mac config command on each laptop. The Setup window's
   **Issue Token** and **Copy Mac Config** buttons produce the same command.

3. **Bring up each laptop**

   ```bash
   brew tap roughcoder/infinite-stack
   brew trust --formula roughcoder/infinite-stack/jarvis
   brew trust --cask roughcoder/infinite-stack/jarvis-app
   brew install jarvis
   brew install --cask jarvis-app
   /usr/bin/xattr -dr com.apple.quarantine /Applications/Jarvis.app
   open -a Jarvis
   ```

   For a totally fresh Mac, install Homebrew first using the official Homebrew installer, then run the Homebrew commands above.

   Paste that laptop's generated Mac config command, choose **Laptop** in Setup,
   run **Check Brain**, run **Check Worker**, then install services. Capture
   evidence:

   ```bash
   jarvis bringup --json --role intercom --role worker --hardware \
     --brain-host imac.private --output ~/Desktop/jarvis-bringup-evidence
   ```

4. **Issue and run the Pi installer**

   From the iMac:

   ```bash
   jarvis pair kitchen-pi --json --pi-installer --brain-host imac.private
   ```

   Run the generated command on the Pi. It must include `JARVIS_REF=v0.1.21`.
   Then on the Pi:

   ```bash
   jarvis-pi doctor
   jarvis-pi status
   jarvis bringup --json --role intercom --platform systemd --hardware \
     --brain-host imac.private --output ~/Desktop/jarvis-bringup-evidence
   ```

5. **Collect iMac evidence and summarize**

   ```bash
   jarvis bringup --json --role brain --role worker --role intercom --hardware \
     --brain-host imac.private --output ~/Desktop/jarvis-bringup-evidence
   jarvis bringup-summary ~/Desktop/jarvis-bringup-evidence \
     --expect-role brain --expect-role worker --expect-role intercom \
     --expect-current-release --min-files 4 \
     --output ~/Desktop/jarvis-bringup-evidence/jarvis-fleet-summary.json
   ```

6. **Prove updates**

   On one Mac:

   ```bash
   brew update
   brew upgrade jarvis
   jarvis service sync brain worker intercom
   brew upgrade --cask jarvis-app
   ```

   On the Pi:

   ```bash
   sudo jarvis-pi update
   ```

   For update proof, create a fresh evidence folder or archive the pre-update
   JSON files before collecting new evidence. Re-run `jarvis bringup-summary ...`
   against only the newest evidence files. The summary should report one runtime
   version and one release ref across every evidence file.

## iMac Brain

Install:

```bash
brew tap roughcoder/infinite-stack
brew trust --formula roughcoder/infinite-stack/jarvis
brew trust --cask roughcoder/infinite-stack/jarvis-app
brew install jarvis
brew install --cask jarvis-app
/usr/bin/xattr -dr com.apple.quarantine /Applications/Jarvis.app
open -a Jarvis
```

Jarvis Setup on the brain Mac can write `BRAIN_HOST=0.0.0.0` and issued device
entries into `~/.jarvis/.env` automatically. Manual equivalent:

```bash
BRAIN_HOST=0.0.0.0
BRAIN_DEVICES=[{"token":"issued-token","device_id":"example-device"}]
```

Use Jarvis Setup:

1. Choose **Brain Mac**.
2. Set the private-network brain hostname, for example `imac.private`.
3. Install services.
4. Issue tokens for each laptop and Pi. The app writes `BRAIN_HOST=0.0.0.0`
   and the `BRAIN_DEVICES` entry to `~/.jarvis/.env` when **Brain Mac** roles
   are selected.

Proof:

```bash
jarvis bringup --json --role brain --role worker --role intercom --hardware \
  --brain-host imac.private --output ~/Desktop/jarvis-bringup-evidence
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
brew tap roughcoder/infinite-stack
brew trust --formula roughcoder/infinite-stack/jarvis
brew trust --cask roughcoder/infinite-stack/jarvis-app
brew install jarvis
brew install --cask jarvis-app
/usr/bin/xattr -dr com.apple.quarantine /Applications/Jarvis.app
open -a Jarvis
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
such as `JARVIS_REF=v0.1.21`, not a development-only `main` ref.

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
- `rpicam-hello --list-cameras` or legacy `libcamera-hello --list-cameras`
  lists the camera when the camera tool is installed and hardware is attached.
- `jarvis-pi doctor` reports a framebuffer, DRM card, or `vcgencmd`
  display-power result for the attached screen.
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

- one clean iMac install completes through Homebrew and Setup
- two clean laptop installs complete through Homebrew and Setup
- one clean Pi install completes from a generated release-pinned command
- all selected services survive a restart
- all device pairing checks pass over the private network
- every evidence file reports the same `jarvis_version` and `release_ref`
- one runtime update succeeds on a Mac through Homebrew
- one app update succeeds through the cask or in-app Homebrew updater
- one Pi update succeeds through `sudo jarvis-pi update`
- `jarvis bringup-summary ~/Desktop/jarvis-bringup-evidence --expect-role brain
  --expect-role worker --expect-role intercom --expect-current-release
  --min-files 4 --output
  ~/Desktop/jarvis-bringup-evidence/jarvis-fleet-summary.json` passes
