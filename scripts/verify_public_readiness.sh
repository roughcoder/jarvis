#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEV_DIR="$(cd "$ROOT_DIR/.." && pwd)"
APPLE_DIR="${JARVIS_APP_DIR:-$DEV_DIR/jarvis-apple}"
TAP_DIR="${JARVIS_TAP_DIR:-$DEV_DIR/homebrew-infinite-stack}"
RUNTIME_VERSION=""
APP_VERSION=""

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

load_release_versions() {
  RUNTIME_VERSION="$(
    python3 - <<'PY'
import tomllib
with open("pyproject.toml", "rb") as handle:
    print(tomllib.load(handle)["project"]["version"])
PY
  )"
  APP_VERSION="$(
    sed -nE 's/^[[:space:]]*version "([^"]+)".*/\1/p' "$TAP_DIR/Casks/jarvis-app.rb" | head -n 1
  )"
  if [[ -z "$RUNTIME_VERSION" || -z "$APP_VERSION" ]]; then
    echo "Could not derive runtime/app versions for public-readiness checks." >&2
    exit 1
  fi
}

scan_release_manifests() {
  section "release manifest scan"
  local init_version lock_version formula_url expected_formula_url

  init_version="$(
    python3 - <<'PY'
import ast
tree = ast.parse(open("src/jarvis/__init__.py", encoding="utf-8").read())
for node in tree.body:
    if isinstance(node, ast.Assign):
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "__version__":
                print(ast.literal_eval(node.value))
                raise SystemExit
raise SystemExit("__version__ not found")
PY
  )"
  lock_version="$(
    python3 - <<'PY'
import tomllib
with open("uv.lock", "rb") as handle:
    packages = tomllib.load(handle)["package"]
for package in packages:
    if package.get("name") == "jarvis" and package.get("source", {}).get("editable") == ".":
        print(package["version"])
        raise SystemExit
raise SystemExit("editable jarvis package not found in uv.lock")
PY
  )"
  if [[ "$init_version" != "$RUNTIME_VERSION" || "$lock_version" != "$RUNTIME_VERSION" ]]; then
    echo "Runtime version mismatch: pyproject=$RUNTIME_VERSION __version__=$init_version uv.lock=$lock_version"
    exit 1
  fi

  expected_formula_url="https://github.com/roughcoder/jarvis/releases/download/v$RUNTIME_VERSION/jarvis-$RUNTIME_VERSION.tar.gz"
  formula_url="$(
    sed -nE 's/^[[:space:]]*url "([^"]+)".*/\1/p' "$TAP_DIR/Formula/jarvis.rb" | head -n 1
  )"
  if [[ "$formula_url" != "$expected_formula_url" ]]; then
    echo "Runtime formula URL mismatch: expected $expected_formula_url, found $formula_url"
    exit 1
  fi
  if ! git -C "$ROOT_DIR" grep -q "REF=\"\${JARVIS_REF:-v$RUNTIME_VERSION}\"" -- scripts/install_pi.sh; then
    echo "Pi installer default JARVIS_REF must match v$RUNTIME_VERSION."
    exit 1
  fi
  if ! git -C "$ROOT_DIR" grep -q "JARVIS_REF=v$RUNTIME_VERSION" -- scripts/install_pi.sh; then
    echo "Pi installer usage text must show the current release ref."
    exit 1
  fi

  if ! git -C "$TAP_DIR" grep -q "version \"$APP_VERSION\"" -- Casks/jarvis-app.rb; then
    echo "App cask version could not be verified: $APP_VERSION"
    exit 1
  fi
  if ! git -C "$TAP_DIR" grep -q 'https://github.com/roughcoder/jarvis-apple/releases/download/v#{version}/Jarvis-macos.zip' -- Casks/jarvis-app.rb; then
    echo "App cask must use the public GitHub release download URL."
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

scan_docs_preview() {
  section "docs preview scan"
  local stale_patterns missing_patterns
  stale_patterns="$(
    {
      git -C "$ROOT_DIR" grep -nE 'brew install --HEAD jarvis|jarvis pair [^<[:space:]]+[[:space:]]+--json</code>|Tailscale|Mac mini' -- docs-site README.md docs/DEPLOYMENT.md docs/FLEET.md docs/PI.md
      git -C "$ROOT_DIR" grep -n 'raw.githubusercontent.com/roughcoder/jarvis/main/scripts/install_pi.sh' -- docs/DEPLOYMENT.md docs/PI.md docs/BRINGUP.md docs/FLEET.md
    } || true
  )"
  if [[ -n "$stale_patterns" ]]; then
    echo "Stale deployment preview/docs patterns found:"
    echo "$stale_patterns"
    exit 1
  fi

  missing_patterns="$(
    {
      git -C "$ROOT_DIR" grep -q 'brew install jarvis' -- README.md docs/DEPLOYMENT.md docs/BRINGUP.md docs/FLEET.md docs-site/index.html || echo "runtime docs missing Homebrew runtime install command"
      git -C "$ROOT_DIR" grep -q 'brew install --cask jarvis-app' -- README.md docs/DEPLOYMENT.md docs/BRINGUP.md docs/FLEET.md docs-site/index.html || echo "runtime docs missing Homebrew app install command"
      git -C "$ROOT_DIR" grep -q 'curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/main/scripts/uninstall_mac.sh | bash' -- README.md docs/DEPLOYMENT.md docs-site/index.html || echo "runtime docs missing public Mac clean-reset command"
      git -C "$APPLE_DIR" grep -q 'brew install jarvis' -- README.md || echo "app docs missing Homebrew runtime install command"
      git -C "$APPLE_DIR" grep -q 'brew install --cask jarvis-app' -- README.md || echo "app docs missing Homebrew app install command"
      git -C "$APPLE_DIR" grep -q 'curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/main/scripts/uninstall_mac.sh | bash' -- README.md || echo "app docs missing public Mac clean-reset command"
      git -C "$TAP_DIR" grep -q 'brew install jarvis' -- README.md || echo "tap docs missing Homebrew runtime install command"
      git -C "$TAP_DIR" grep -q 'brew install --cask jarvis-app' -- README.md || echo "tap docs missing Homebrew app install command"
      git -C "$TAP_DIR" grep -q 'curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/main/scripts/uninstall_mac.sh | bash' -- README.md || echo "tap docs missing public Mac clean-reset command"
      git -C "$ROOT_DIR" grep -q "jarvis $RUNTIME_VERSION" -- docs-site/index.html || echo "docs-site/index.html missing current runtime release"
      git -C "$ROOT_DIR" grep -q "jarvis-app $APP_VERSION" -- docs-site/index.html || echo "docs-site/index.html missing current app release"
      git -C "$ROOT_DIR" grep -q "JARVIS_REF=v$RUNTIME_VERSION" -- docs-site/index.html || echo "docs-site/index.html missing current Pi release ref"
      git -C "$ROOT_DIR" grep -q 'Fresh fleet runbook' -- docs-site/index.html || echo "docs-site/index.html missing fresh fleet runbook section"
      git -C "$ROOT_DIR" grep -q 'brew trust --formula roughcoder/infinite-stack/jarvis' -- README.md docs/DEPLOYMENT.md || echo "runtime docs missing entry-specific formula trust command"
      git -C "$ROOT_DIR" grep -q 'brew trust --cask roughcoder/infinite-stack/jarvis-app' -- README.md docs/DEPLOYMENT.md || echo "runtime docs missing entry-specific cask trust command"
      git -C "$APPLE_DIR" grep -q 'brew trust --formula roughcoder/infinite-stack/jarvis' -- README.md || echo "app docs missing entry-specific formula trust command"
      git -C "$APPLE_DIR" grep -q 'brew trust --cask roughcoder/infinite-stack/jarvis-app' -- README.md || echo "app docs missing entry-specific cask trust command"
      git -C "$TAP_DIR" grep -q 'brew trust --formula roughcoder/infinite-stack/jarvis' -- README.md || echo "tap docs missing entry-specific formula trust command"
      git -C "$TAP_DIR" grep -q 'brew trust --cask roughcoder/infinite-stack/jarvis-app' -- README.md || echo "tap docs missing entry-specific cask trust command"
      git -C "$ROOT_DIR" grep -q 'jarvis service sync brain worker intercom' -- docs-site/index.html || echo "docs-site/index.html missing role sync command"
      git -C "$ROOT_DIR" grep -q -- '--pi-installer --brain-host imac.private' -- docs-site/index.html || echo "docs-site/index.html missing release-style Pi pairing command"
      git -C "$ROOT_DIR" grep -q -- '--brain-host imac.private --output ~/Desktop/jarvis-bringup-evidence' -- docs-site/index.html || echo "docs-site/index.html missing brain host in bring-up evidence command"
      git -C "$ROOT_DIR" grep -q -- '--output ~/Desktop/jarvis-bringup-evidence' -- docs-site/index.html || echo "docs-site/index.html missing bring-up evidence output command"
      git -C "$ROOT_DIR" grep -q -- '--expect-current-release --min-files 4 --output ~/Desktop/jarvis-bringup-evidence/jarvis-fleet-summary.json' -- docs-site/index.html || echo "docs-site/index.html missing current-release bring-up summary output command"
      git -C "$ROOT_DIR" grep -q 'sudo jarvis-pi update' -- docs-site/index.html || echo "docs-site/index.html missing Pi update command"
      git -C "$ROOT_DIR" grep -q 'actions/deploy-pages@v4' -- .github/workflows/pages.yml || echo ".github/workflows/pages.yml missing Pages deploy action"
    } || true
  )"
  if [[ -n "$missing_patterns" ]]; then
    echo "Deployment preview is missing required install/update guidance:"
    echo "$missing_patterns"
    exit 1
  fi
}

require_dir "$ROOT_DIR" "Jarvis runtime"
require_dir "$APPLE_DIR" "Jarvis app"
require_dir "$TAP_DIR" "Homebrew tap"
load_release_versions

section "clean worktrees"
git -C "$ROOT_DIR" status --short
git -C "$APPLE_DIR" status --short
git -C "$TAP_DIR" status --short

if [[ -n "$(git -C "$ROOT_DIR" status --short)" || -n "$(git -C "$APPLE_DIR" status --short)" || -n "$(git -C "$TAP_DIR" status --short)" ]]; then
  echo "One or more worktrees are dirty. Commit or stash before public release."
  exit 1
fi

scan_release_manifests
scan_runtime_public_files
scan_app_public_files
scan_tap_public_files
scan_docs_preview

section "workflow lint"
if command -v actionlint >/dev/null 2>&1; then
  (cd "$ROOT_DIR" && actionlint)
  (cd "$APPLE_DIR" && actionlint)
  (cd "$TAP_DIR" && actionlint)
else
  echo "actionlint not installed; skipping workflow lint"
fi

section "shell lint"
if command -v shellcheck >/dev/null 2>&1; then
  (cd "$ROOT_DIR" && shellcheck scripts/uninstall_mac.sh scripts/install_pi.sh scripts/sync_runtime_check_env.sh scripts/release_runtime.sh scripts/update_homebrew_formula.sh scripts/verify_public_readiness.sh)
  (cd "$APPLE_DIR" && shellcheck scripts/install_latest.sh scripts/release_github.sh scripts/build_release.sh scripts/update_homebrew_cask.sh)
else
  echo "shellcheck not installed; skipping shell lint"
fi

section "runtime checks"
"$ROOT_DIR/scripts/sync_runtime_check_env.sh"
(cd "$ROOT_DIR" && uv run ruff check src/ tests/)
(cd "$ROOT_DIR" && bash -n scripts/uninstall_mac.sh scripts/install_pi.sh scripts/sync_runtime_check_env.sh scripts/release_runtime.sh scripts/update_homebrew_formula.sh)
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
