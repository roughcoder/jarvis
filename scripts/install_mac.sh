#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: install_mac.sh

Installs or updates the Jarvis runtime and native macOS app on a fresh Mac.
After installation it opens Jarvis so first-run setup can choose roles, install
services, and issue pairing entries.

Environment:
  JARVIS_TAP=roughcoder/infinite-stack      Homebrew tap to install from.
  JARVIS_RUNTIME_FORMULA=jarvis             Runtime formula token.
  JARVIS_APP_CASK=jarvis-app                Native app cask token.
  JARVIS_ROLES="brain worker intercom"      Optional roles to install locally.
  JARVIS_WORKDIR=$HOME/.jarvis              Workdir/config dir for installed services.
  JARVIS_START_SERVICES=0                   Start optional roles after install.
  JARVIS_OPEN_APP=1                         Open Jarvis.app after install.
  JARVIS_INSTALL_HOMEBREW=1                 Install Homebrew when missing.
  JARVIS_ALLOW_HEAD_FALLBACK=0              Fall back to formula HEAD on runtime install failure.
  JARVIS_BREW_PATH=/opt/homebrew/bin/brew   Override brew path.
  JARVIS_DRY_RUN=0                          Print commands instead of running.
  JARVIS_ASSUME_MAC=0                       Skip uname check for tests.
  JARVIS_DRY_RUN_RUNTIME_INSTALLED=0        Dry-run runtime install state.
  JARVIS_DRY_RUN_APP_INSTALLED=0            Dry-run app install state.

Examples:
  curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/main/scripts/install_mac.sh | bash
  JARVIS_ROLES="intercom worker" JARVIS_START_SERVICES=1 bash install_mac.sh
  JARVIS_DRY_RUN=1 bash install_mac.sh
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${JARVIS_ASSUME_MAC:-0}" != "1" && "$(uname -s)" != "Darwin" ]]; then
  echo "install_mac.sh only supports macOS." >&2
  exit 1
fi

TAP="${JARVIS_TAP:-roughcoder/infinite-stack}"
RUNTIME_FORMULA="${JARVIS_RUNTIME_FORMULA:-jarvis}"
APP_CASK="${JARVIS_APP_CASK:-jarvis-app}"
ROLES="${JARVIS_ROLES:-}"
WORKDIR="${JARVIS_WORKDIR:-$HOME/.jarvis}"
START_SERVICES="${JARVIS_START_SERVICES:-0}"
OPEN_APP="${JARVIS_OPEN_APP:-1}"
INSTALL_HOMEBREW="${JARVIS_INSTALL_HOMEBREW:-1}"
ALLOW_HEAD_FALLBACK="${JARVIS_ALLOW_HEAD_FALLBACK:-0}"
DRY_RUN="${JARVIS_DRY_RUN:-0}"
DRY_RUN_RUNTIME_INSTALLED="${JARVIS_DRY_RUN_RUNTIME_INSTALLED:-0}"
DRY_RUN_APP_INSTALLED="${JARVIS_DRY_RUN_APP_INSTALLED:-0}"

run() {
  if [[ "$DRY_RUN" == "1" ]]; then
    printf '+'
    printf ' %q' "$@"
    printf '\n'
  else
    "$@"
  fi
}

find_brew() {
  if [[ -n "${JARVIS_BREW_PATH:-}" ]]; then
    echo "$JARVIS_BREW_PATH"
    return 0
  fi
  if command -v brew >/dev/null 2>&1; then
    command -v brew
    return 0
  fi
  for candidate in /opt/homebrew/bin/brew /usr/local/bin/brew; do
    if [[ -x "$candidate" ]]; then
      echo "$candidate"
      return 0
    fi
  done
  return 1
}

if ! BREW_PATH="$(find_brew)"; then
  if [[ "$INSTALL_HOMEBREW" != "1" ]]; then
    echo "Homebrew is required. Install it first or set JARVIS_INSTALL_HOMEBREW=1." >&2
    exit 1
  fi
  echo "Installing Homebrew"
  if [[ "$DRY_RUN" == "1" ]]; then
    run /bin/bash -c "<homebrew install script>"
    BREW_PATH="${JARVIS_BREW_PATH:-/opt/homebrew/bin/brew}"
  else
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    BREW_PATH="$(find_brew)"
  fi
fi

if [[ "$DRY_RUN" != "1" ]]; then
  eval "$("$BREW_PATH" shellenv)"
  BREW_PATH="$(command -v brew)"
fi

echo "Updating Homebrew metadata"
run "$BREW_PATH" update

echo "Tapping $TAP"
run "$BREW_PATH" tap "$TAP"

echo "Installing Jarvis runtime"
if [[ "$DRY_RUN" == "1" ]]; then
  run "$BREW_PATH" list --formula "$RUNTIME_FORMULA"
  if [[ "$DRY_RUN_RUNTIME_INSTALLED" == "1" ]]; then
    run "$BREW_PATH" upgrade "$RUNTIME_FORMULA"
    if [[ "$ALLOW_HEAD_FALLBACK" == "1" ]]; then
      run "$BREW_PATH" upgrade --fetch-HEAD "$RUNTIME_FORMULA"
    fi
  else
    run "$BREW_PATH" install "$RUNTIME_FORMULA"
    if [[ "$ALLOW_HEAD_FALLBACK" == "1" ]]; then
      run "$BREW_PATH" install --HEAD "$RUNTIME_FORMULA"
    fi
  fi
elif "$BREW_PATH" list --formula "$RUNTIME_FORMULA" >/dev/null 2>&1; then
  if ! "$BREW_PATH" upgrade "$RUNTIME_FORMULA"; then
    if [[ "$ALLOW_HEAD_FALLBACK" == "1" ]]; then
      "$BREW_PATH" upgrade --fetch-HEAD "$RUNTIME_FORMULA" || true
    else
      exit 1
    fi
  fi
else
  if ! "$BREW_PATH" install "$RUNTIME_FORMULA"; then
    if [[ "$ALLOW_HEAD_FALLBACK" == "1" ]]; then
      "$BREW_PATH" install --HEAD "$RUNTIME_FORMULA"
    else
      exit 1
    fi
  fi
fi

echo "Installing Jarvis app"
if [[ "$DRY_RUN" == "1" ]]; then
  run "$BREW_PATH" list --cask "$APP_CASK"
  if [[ "$DRY_RUN_APP_INSTALLED" == "1" ]]; then
    run "$BREW_PATH" upgrade --cask "$APP_CASK"
  else
    run "$BREW_PATH" install --cask "$APP_CASK"
  fi
elif "$BREW_PATH" list --cask "$APP_CASK" >/dev/null 2>&1; then
  "$BREW_PATH" upgrade --cask "$APP_CASK" || true
else
  "$BREW_PATH" install --cask "$APP_CASK"
fi

if [[ -d "/Applications/Jarvis.app" ]]; then
  /usr/bin/xattr -dr com.apple.quarantine /Applications/Jarvis.app >/dev/null 2>&1 || true
elif [[ -d "$HOME/Applications/Jarvis.app" ]]; then
  /usr/bin/xattr -dr com.apple.quarantine "$HOME/Applications/Jarvis.app" >/dev/null 2>&1 || true
fi

if [[ -n "$ROLES" ]]; then
  read -r -a ROLE_ARGS <<< "$ROLES"
  echo "Syncing Jarvis role dependencies: $ROLES"
  run jarvis service sync "${ROLE_ARGS[@]}"

  echo "Installing Jarvis services: $ROLES"
  for role in "${ROLE_ARGS[@]}"; do
    run jarvis service install "$role" --workdir "$WORKDIR"
    if [[ "$START_SERVICES" == "1" ]]; then
      if [[ "$DRY_RUN" == "1" ]]; then
        run jarvis service start "$role"
        run jarvis service restart "$role"
      else
        jarvis service start "$role" || jarvis service restart "$role"
      fi
    fi
  done
fi

if [[ "$OPEN_APP" == "1" ]]; then
  echo "Opening Jarvis"
  run /usr/bin/open -a Jarvis || true
fi

cat <<NEXT

Jarvis is installed.

Next:
  1. Open the Jarvis menu bar item.
  2. Choose Setup.
  3. Select this Mac's roles.
  4. Install/start services from the Setup window.
  5. Pair Raspberry Pis or laptops from the brain Mac with "Issue Token".

Update later with:
  brew update
  brew upgrade $RUNTIME_FORMULA
  brew upgrade --cask $APP_CASK

Physical bring-up evidence:
  mkdir -p ~/Desktop/jarvis-bringup-evidence
  Use Jarvis Setup > Collect Evidence and Summarize Evidence, or run:
NEXT

if [[ -n "$ROLES" ]]; then
  printf '  jarvis bringup --json'
  for role in "${ROLE_ARGS[@]}"; do
    printf ' --role %q' "$role"
  done
  printf ' --hardware --output ~/Desktop/jarvis-bringup-evidence\n'
else
  cat <<'NEXT'
  jarvis bringup --json --role brain --role worker --role intercom --hardware \
    --output ~/Desktop/jarvis-bringup-evidence
NEXT
fi

cat <<'NEXT'
  jarvis bringup-summary ~/Desktop/jarvis-bringup-evidence \
    --expect-role brain --expect-role worker --expect-role intercom --min-files 4 \
    --output ~/Desktop/jarvis-bringup-evidence/jarvis-fleet-summary.json
NEXT
