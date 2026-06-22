#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEV_DIR="$(cd "$ROOT_DIR/.." && pwd)"
APPLE_DIR="${JARVIS_APP_DIR:-$DEV_DIR/jarvis-apple}"
TAP_DIR="${JARVIS_TAP_DIR:-$DEV_DIR/homebrew-infinite-stack}"

section() {
  printf '\n==> %s\n' "$1"
}

require_dir() {
  local path="$1"
  local label="$2"
  if [[ ! -d "$path/.git" ]]; then
    echo "$label checkout not found at $path" >&2
    exit 1
  fi
}

scan_runtime_public_files() {
  section "runtime public-file scan"
  local blocked
  blocked="$(
    git -C "$ROOT_DIR" ls-files \
      | grep -E '(^|/)(\.env$|jarvis-workspace/\.mcp-auth/|jarvis-workspace/browser/|jarvis-workspace/users/|jarvis-workspace/worker/jobs/|jarvis-workspace/worker/runs/|\.jsonl$|\.sqlite$|\.db$)' \
      || true
  )"
  if [[ -n "$blocked" ]]; then
    echo "Tracked runtime/private files found:"
    echo "$blocked"
    exit 1
  fi
}

scan_app_public_files() {
  section "app public-file scan"
  local blocked hits
  blocked="$(
    git -C "$APPLE_DIR" ls-files \
      | grep -E '(^|/)(\.env$|\.env\.|dist/|DerivedData/|\.build/|xcuserdata/|\.DS_Store$|.*\.zip$|.*\.dmg$)' \
      || true
  )"
  if [[ -n "$blocked" ]]; then
    echo "Tracked app build/release artifacts found:"
    echo "$blocked"
    exit 1
  fi

  hits="$(
    git -C "$APPLE_DIR" grep -IlE '(ghp_|github_pat_|sk-[A-Za-z0-9]{20,}|BEGIN (RSA|OPENSSH|PRIVATE) KEY)' -- ':!.github/workflows/public-readiness.yml' \
      || true
  )"
  if [[ -n "$hits" ]]; then
    echo "Possible checked-in app secrets found:"
    echo "$hits"
    exit 1
  fi
}

scan_tap_public_files() {
  section "tap public-file scan"
  local private_patterns secret_hits
  private_patterns="$(
    git -C "$TAP_DIR" grep -IlE '(api\.github\.com/repos/.+/releases/assets|HOMEBREW_GITHUB_API_TOKEN|private GitHub release|private testing phase)' -- ':!.github/workflows/public-readiness.yml' \
      || true
  )"
  if [[ -n "$private_patterns" ]]; then
    echo "Private-release Homebrew patterns found:"
    echo "$private_patterns"
    exit 1
  fi

  secret_hits="$(
    git -C "$TAP_DIR" grep -IlE '(ghp_|github_pat_|sk-[A-Za-z0-9]{20,}|BEGIN (RSA|OPENSSH|PRIVATE) KEY)' -- ':!.github/workflows/public-readiness.yml' \
      || true
  )"
  if [[ -n "$secret_hits" ]]; then
    echo "Possible checked-in tap secrets found:"
    echo "$secret_hits"
    exit 1
  fi
}

require_dir "$ROOT_DIR" "Jarvis runtime"
require_dir "$APPLE_DIR" "Jarvis app"
require_dir "$TAP_DIR" "Homebrew tap"

section "clean worktrees"
git -C "$ROOT_DIR" status --short
git -C "$APPLE_DIR" status --short
git -C "$TAP_DIR" status --short

if [[ -n "$(git -C "$ROOT_DIR" status --short)" || -n "$(git -C "$APPLE_DIR" status --short)" || -n "$(git -C "$TAP_DIR" status --short)" ]]; then
  echo "One or more worktrees are dirty. Commit or stash before public release."
  exit 1
fi

scan_runtime_public_files
scan_app_public_files
scan_tap_public_files

section "workflow lint"
if command -v actionlint >/dev/null 2>&1; then
  (cd "$ROOT_DIR" && actionlint)
  (cd "$APPLE_DIR" && actionlint)
  (cd "$TAP_DIR" && actionlint)
else
  echo "actionlint not installed; skipping workflow lint"
fi

section "runtime checks"
(cd "$ROOT_DIR" && uv run ruff check src/ tests/)
(cd "$ROOT_DIR" && bash -n scripts/install_pi.sh)
(cd "$ROOT_DIR" && uv run pytest tests/unit -q)

section "app checks"
(cd "$APPLE_DIR" && bash -n scripts/install_latest.sh scripts/update_homebrew_cask.sh scripts/release_github.sh scripts/build_release.sh)
(cd "$APPLE_DIR" && swift test)

section "tap checks"
(cd "$TAP_DIR" && brew style Formula/jarvis.rb Casks/jarvis-app.rb)
(cd "$TAP_DIR" && brew audit --cask roughcoder/infinite-stack/jarvis-app)
(cd "$TAP_DIR" && brew audit --formula roughcoder/infinite-stack/jarvis)

section "public readiness complete"
echo "Local public-readiness checks passed."
