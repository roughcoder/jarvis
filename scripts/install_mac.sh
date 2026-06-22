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
  JARVIS_START_SERVICES=0                   Start optional roles after install.
  JARVIS_OPEN_APP=1                         Open Jarvis.app after install.
  JARVIS_INSTALL_HOMEBREW=1                 Install Homebrew when missing.

Examples:
  curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/main/scripts/install_mac.sh | bash
  JARVIS_ROLES="intercom worker" JARVIS_START_SERVICES=1 bash install_mac.sh
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "install_mac.sh only supports macOS." >&2
  exit 1
fi

TAP="${JARVIS_TAP:-roughcoder/infinite-stack}"
RUNTIME_FORMULA="${JARVIS_RUNTIME_FORMULA:-jarvis}"
APP_CASK="${JARVIS_APP_CASK:-jarvis-app}"
ROLES="${JARVIS_ROLES:-}"
START_SERVICES="${JARVIS_START_SERVICES:-0}"
OPEN_APP="${JARVIS_OPEN_APP:-1}"
INSTALL_HOMEBREW="${JARVIS_INSTALL_HOMEBREW:-1}"

find_brew() {
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
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  BREW_PATH="$(find_brew)"
fi

eval "$("$BREW_PATH" shellenv)"
BREW_PATH="$(command -v brew)"

echo "Updating Homebrew metadata"
"$BREW_PATH" update

echo "Tapping $TAP"
"$BREW_PATH" tap "$TAP"

echo "Installing Jarvis runtime"
if "$BREW_PATH" list --formula "$RUNTIME_FORMULA" >/dev/null 2>&1; then
  "$BREW_PATH" upgrade "$RUNTIME_FORMULA" || "$BREW_PATH" upgrade --fetch-HEAD "$RUNTIME_FORMULA" || true
else
  "$BREW_PATH" install "$RUNTIME_FORMULA" || "$BREW_PATH" install --HEAD "$RUNTIME_FORMULA"
fi

echo "Installing Jarvis app"
if "$BREW_PATH" list --cask "$APP_CASK" >/dev/null 2>&1; then
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
  echo "Installing Jarvis services: $ROLES"
  for role in $ROLES; do
    jarvis service install "$role"
    if [[ "$START_SERVICES" == "1" ]]; then
      jarvis service start "$role" || jarvis service restart "$role"
    fi
  done
fi

if [[ "$OPEN_APP" == "1" ]]; then
  echo "Opening Jarvis"
  /usr/bin/open -a Jarvis || true
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
NEXT

