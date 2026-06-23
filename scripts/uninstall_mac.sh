#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: uninstall_mac.sh

Cleanly removes Jarvis installed artifacts from this Mac while leaving local
source checkouts alone. This is intended for fresh end-to-end install testing.

Environment:
  JARVIS_TAP=roughcoder/infinite-stack      Homebrew tap name.
  JARVIS_RUNTIME_FORMULA=jarvis             Runtime formula token.
  JARVIS_APP_CASK=jarvis-app                Native app cask token.
  JARVIS_WORKDIR=$HOME/.jarvis              Runtime workdir/config dir.
  JARVIS_LOG_DIR=$HOME/Library/Logs/Jarvis  Runtime/app log dir.
  JARVIS_UNINSTALL_PACKAGES=1               Remove Homebrew app + runtime.
  JARVIS_REMOVE_CONFIG=1                    Remove ~/.jarvis.
  JARVIS_REMOVE_LOGS=1                      Remove Jarvis logs.
  JARVIS_REMOVE_APP_SETTINGS=1              Remove app preferences/caches.
  JARVIS_DRY_RUN=0                          Print commands instead of running.
  JARVIS_ASSUME_MAC=0                       Skip uname check for tests.
  JARVIS_BREW_PATH=/opt/homebrew/bin/brew   Override brew path.

Examples:
  bash scripts/uninstall_mac.sh
  JARVIS_DRY_RUN=1 bash scripts/uninstall_mac.sh
  JARVIS_UNINSTALL_PACKAGES=0 bash scripts/uninstall_mac.sh
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${JARVIS_ASSUME_MAC:-0}" != "1" && "$(uname -s)" != "Darwin" ]]; then
  echo "uninstall_mac.sh only supports macOS." >&2
  exit 1
fi

TAP="${JARVIS_TAP:-roughcoder/infinite-stack}"
RUNTIME_FORMULA="${JARVIS_RUNTIME_FORMULA:-jarvis}"
APP_CASK="${JARVIS_APP_CASK:-jarvis-app}"
WORKDIR="${JARVIS_WORKDIR:-$HOME/.jarvis}"
LOG_DIR="${JARVIS_LOG_DIR:-$HOME/Library/Logs/Jarvis}"
UNINSTALL_PACKAGES="${JARVIS_UNINSTALL_PACKAGES:-1}"
REMOVE_CONFIG="${JARVIS_REMOVE_CONFIG:-1}"
REMOVE_LOGS="${JARVIS_REMOVE_LOGS:-1}"
REMOVE_APP_SETTINGS="${JARVIS_REMOVE_APP_SETTINGS:-1}"
DRY_RUN="${JARVIS_DRY_RUN:-0}"

run() {
  if [[ "$DRY_RUN" == "1" ]]; then
    printf '+'
    printf ' %q' "$@"
    printf '\n'
  else
    "$@"
  fi
}

run_may_fail() {
  if [[ "$DRY_RUN" == "1" ]]; then
    run "$@"
  else
    "$@" >/dev/null 2>&1 || true
  fi
}

remove_path() {
  local path="$1"
  if [[ "$DRY_RUN" == "1" ]]; then
    run rm -rf "$path"
  elif [[ -e "$path" || -L "$path" ]]; then
    rm -rf "$path"
    echo "Removed $path"
  else
    echo "Already absent: $path"
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

echo "Quitting Jarvis app"
run_may_fail /usr/bin/osascript -e 'tell application "Jarvis" to quit'

uid="$(id -u)"
for role in brain intercom worker; do
  plist="$HOME/Library/LaunchAgents/com.jarvis.$role.plist"
  echo "Stopping $role service"
  run_may_fail /bin/launchctl bootout "gui/$uid" "$plist"
  remove_path "$plist"
done

if [[ "$REMOVE_CONFIG" == "1" ]]; then
  remove_path "$WORKDIR"
fi

if [[ "$REMOVE_LOGS" == "1" ]]; then
  remove_path "$LOG_DIR"
fi

if [[ "$REMOVE_APP_SETTINGS" == "1" ]]; then
  remove_path "$HOME/Library/Preferences/dev.infinitestack.jarvis.mac.plist"
  remove_path "$HOME/Library/Saved Application State/dev.infinitestack.jarvis.mac.savedState"
  remove_path "$HOME/Library/Caches/dev.infinitestack.jarvis.mac"
  remove_path "$HOME/Library/Application Support/Jarvis"
  remove_path "$HOME/Library/Application Support/dev.infinitestack.jarvis.mac"
fi

if [[ "$UNINSTALL_PACKAGES" == "1" ]]; then
  if BREW_PATH="$(find_brew)"; then
    if [[ "$DRY_RUN" != "1" ]]; then
      eval "$("$BREW_PATH" shellenv)"
      BREW_PATH="$(command -v brew)"
    fi
    echo "Uninstalling Homebrew app/runtime packages"
    run_may_fail "$BREW_PATH" uninstall --cask --zap "$APP_CASK"
    run_may_fail "$BREW_PATH" uninstall --formula "$RUNTIME_FORMULA"
  else
    echo "Homebrew not found; removing app bundles directly"
  fi
  remove_path "/Applications/Jarvis.app"
  remove_path "$HOME/Applications/Jarvis.app"
fi

cat <<NEXT

Jarvis uninstall complete.

Local source checkouts were not touched. To reinstall with Homebrew:

  brew tap $TAP
  brew trust --formula $TAP/$RUNTIME_FORMULA
  brew trust --cask $TAP/$APP_CASK
  brew install $RUNTIME_FORMULA
  brew install --cask $APP_CASK
  /usr/bin/xattr -dr com.apple.quarantine /Applications/Jarvis.app
  open -a Jarvis
NEXT
