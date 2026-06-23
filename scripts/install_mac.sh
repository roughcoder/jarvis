#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: install_mac.sh

Installs or updates the Jarvis runtime and native macOS app with Homebrew,
prepares the local Jarvis workdir, clears app quarantine, and opens Jarvis.

Environment:
  JARVIS_TAP=roughcoder/infinite-stack      Homebrew tap to install from.
  JARVIS_RUNTIME_FORMULA=jarvis             Runtime formula token.
  JARVIS_APP_CASK=jarvis-app                Native app cask token.
  JARVIS_WORKDIR=$HOME/.jarvis              Local workdir/config dir.
  JARVIS_OPEN_APP=1                         Open Jarvis.app after install.
  JARVIS_INSTALL_HOMEBREW=1                 Install Homebrew when missing.
  JARVIS_BREW_PATH=/opt/homebrew/bin/brew   Override brew path.
  JARVIS_DRY_RUN=0                          Print commands instead of running.
  JARVIS_ASSUME_MAC=0                       Skip uname check for tests.

Examples:
  curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/main/scripts/install_mac.sh | bash
  JARVIS_DRY_RUN=1 bash scripts/install_mac.sh
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
WORKDIR="${JARVIS_WORKDIR:-$HOME/.jarvis}"
OPEN_APP="${JARVIS_OPEN_APP:-1}"
INSTALL_HOMEBREW="${JARVIS_INSTALL_HOMEBREW:-1}"
DRY_RUN="${JARVIS_DRY_RUN:-0}"

run() {
  if [[ "$DRY_RUN" == "1" ]]; then
    printf '+'
    printf ' %q' "$@"
    printf '\n'
  else
    "$@" </dev/null
  fi
}

installed_app_path() {
  if [[ -d "/Applications/Jarvis.app" ]]; then
    echo "/Applications/Jarvis.app"
    return 0
  fi
  if [[ -d "$HOME/Applications/Jarvis.app" ]]; then
    echo "$HOME/Applications/Jarvis.app"
    return 0
  fi
  return 1
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
    NONINTERACTIVE=1 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)" </dev/null
    BREW_PATH="$(find_brew)"
  fi
fi

if [[ "$DRY_RUN" != "1" ]]; then
  eval "$("$BREW_PATH" shellenv </dev/null)"
  BREW_PATH="$(command -v brew)"
fi

echo "Tapping $TAP"
run "$BREW_PATH" tap "$TAP"

echo "Updating $TAP"
if [[ "$DRY_RUN" == "1" ]]; then
  run /usr/bin/git -C "<homebrew tap repo>" pull --ff-only
else
  TAP_REPO="$("$BREW_PATH" --repo "$TAP" </dev/null)"
  if [[ -n "$TAP_REPO" && -d "$TAP_REPO/.git" ]]; then
    run /usr/bin/git -C "$TAP_REPO" pull --ff-only
  fi
fi

echo "Trusting Jarvis Homebrew entries"
if [[ "$DRY_RUN" == "1" ]]; then
  run "$BREW_PATH" trust --formula "$TAP/$RUNTIME_FORMULA"
  run "$BREW_PATH" trust --cask "$TAP/$APP_CASK"
elif "$BREW_PATH" help trust >/dev/null 2>&1 </dev/null; then
  run "$BREW_PATH" trust --formula "$TAP/$RUNTIME_FORMULA" || true
  run "$BREW_PATH" trust --cask "$TAP/$APP_CASK" || true
fi

echo "Installing Jarvis runtime"
if [[ "$DRY_RUN" == "1" ]]; then
  run "$BREW_PATH" install "$RUNTIME_FORMULA"
else
  run "$BREW_PATH" install "$RUNTIME_FORMULA" || run "$BREW_PATH" upgrade "$RUNTIME_FORMULA"
fi

echo "Installing Jarvis app"
if [[ "$DRY_RUN" == "1" ]]; then
  run "$BREW_PATH" install --cask "$APP_CASK"
else
  run "$BREW_PATH" install --cask "$APP_CASK" || run "$BREW_PATH" upgrade --cask "$APP_CASK" || true
fi

echo "Preparing Jarvis workdir"
run /bin/mkdir -p "$WORKDIR"

echo "Clearing app quarantine"
if [[ "$DRY_RUN" == "1" ]]; then
  run /usr/bin/xattr -dr com.apple.quarantine /Applications/Jarvis.app
else
  if [[ -d "/Applications/Jarvis.app" ]]; then
    /usr/bin/xattr -dr com.apple.quarantine /Applications/Jarvis.app >/dev/null 2>&1 || true
  elif [[ -d "$HOME/Applications/Jarvis.app" ]]; then
    /usr/bin/xattr -dr com.apple.quarantine "$HOME/Applications/Jarvis.app" >/dev/null 2>&1 || true
  fi
fi

if [[ "$OPEN_APP" == "1" ]]; then
  echo "Opening Jarvis"
  if [[ "$DRY_RUN" == "1" ]]; then
    run /usr/bin/open /Applications/Jarvis.app || true
  elif APP_PATH="$(installed_app_path)"; then
    run /usr/bin/open "$APP_PATH" || true
  else
    echo "Jarvis app was installed, but no app bundle was found to open." >&2
  fi
fi

cat <<NEXT

Jarvis is installed.

Next:
  1. Open the Jarvis menu bar item.
  2. Choose Setup.
  3. Select this Mac's profile.
  4. Press Install Services.
  5. Pair laptops or Raspberry Pis from the brain Mac with Issue Token.

Clean reset:
  curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/main/scripts/uninstall_mac.sh | bash
NEXT
