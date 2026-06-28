#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/release_runtime.sh <version> [--draft] [--skip-homebrew]

Internal release publisher for .github/workflows/release.yml.

Builds a source tarball from the checked-out GitHub Actions commit, pushes the
tag, creates or updates a GitHub Release, and optionally updates the Homebrew
formula. Do not run this script locally; trigger the Release workflow instead.

Environment:
  GITHUB_REPOSITORY=owner/repo      Override repository detection.
  SKIP_HOMEBREW=1                  Do not update the Homebrew formula.
  HOMEBREW_TAP_DIR=/path/to/tap    Override the local Homebrew tap checkout.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

VERSION="${1:-}"
if [[ -z "$VERSION" ]]; then
  usage >&2
  exit 2
fi
shift || true

if [[ "${GITHUB_ACTIONS:-}" != "true" || "${GITHUB_WORKFLOW:-}" != "Release" ]]; then
  cat >&2 <<'MSG'
Runtime releases must be published by the GitHub Actions "Release" workflow.
Do not run scripts/release_runtime.sh locally; local releases can create version,
tag, asset, and Homebrew formula mismatches.
MSG
  exit 2
fi

DRAFT_FLAG=""
SKIP_HOMEBREW="${SKIP_HOMEBREW:-0}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --draft)
      DRAFT_FLAG="--draft"
      ;;
    --skip-homebrew)
      SKIP_HOMEBREW=1
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

VERSION="${VERSION#v}"
TAG="v$VERSION"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="$ROOT_DIR/dist"
ASSET_NAME="jarvis-$VERSION.tar.gz"
ASSET_PATH="$DIST_DIR/$ASSET_NAME"
cd "$ROOT_DIR"

if ! [[ "$VERSION" =~ ^[0-9]+[.][0-9]+[.][0-9]+([-+][0-9A-Za-z.-]+)?$ ]]; then
  echo "Version must look like 1.2.3, optionally prefixed with v." >&2
  exit 2
fi

PYPROJECT_VERSION="$(python3 - <<'PY'
import tomllib
with open("pyproject.toml", "rb") as handle:
    print(tomllib.load(handle)["project"]["version"])
PY
)"
INIT_VERSION="$(python3 - <<'PY'
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
if [[ "$PYPROJECT_VERSION" != "$VERSION" || "$INIT_VERSION" != "$VERSION" ]]; then
  echo "Version mismatch: pyproject=$PYPROJECT_VERSION __version__=$INIT_VERSION release=$VERSION" >&2
  exit 1
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "GitHub CLI is required: brew install gh" >&2
  exit 1
fi

gh auth status >/dev/null

REPOSITORY="${GITHUB_REPOSITORY:-}"
if [[ -z "$REPOSITORY" ]]; then
  REPOSITORY="$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)"
fi
if [[ -z "$REPOSITORY" ]]; then
  echo "Could not detect GitHub repository. Set GITHUB_REPOSITORY=owner/repo." >&2
  exit 1
fi

if [[ -n "$(git status --porcelain -- . ':(exclude)dist')" ]]; then
  echo "Working tree has uncommitted source changes. Commit or stash before releasing." >&2
  git status --short -- . ':(exclude)dist'
  exit 1
fi

"$ROOT_DIR/scripts/sync_runtime_check_env.sh"
uv run ruff check src/ tests/ scripts/generate_release_notes.py
bash -n scripts/install_mac.sh scripts/uninstall_mac.sh scripts/install_pi.sh scripts/sync_runtime_check_env.sh scripts/verify_public_readiness.sh scripts/release_runtime.sh scripts/update_homebrew_formula.sh
uv run pytest tests/unit -q

rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"
git archive --format=tar.gz --prefix="jarvis-$VERSION/" -o "$ASSET_PATH" HEAD
shasum -a 256 "$ASSET_PATH" > "$ASSET_PATH.sha256"

uv run python scripts/generate_release_notes.py \
  --version "$VERSION" \
  --output "$DIST_DIR/runtime-release-notes.md"

if ! git rev-parse "$TAG" >/dev/null 2>&1; then
  git tag -a "$TAG" -m "Release $TAG"
fi

CURRENT_BRANCH="$(git branch --show-current)"
if [[ -n "$CURRENT_BRANCH" ]]; then
  git push origin "$CURRENT_BRANCH"
else
  echo "Detached HEAD: skipping branch push; ensure the release commit is already reachable from origin."
fi
git push origin "$TAG"

if gh release view "$TAG" --repo "$REPOSITORY" >/dev/null 2>&1; then
  gh release upload "$TAG" \
    "$ASSET_PATH" \
    "$ASSET_PATH.sha256" \
    --repo "$REPOSITORY" \
    --clobber
  gh release edit "$TAG" \
    --repo "$REPOSITORY" \
    --title "Jarvis Runtime $TAG" \
    --notes-file "$DIST_DIR/runtime-release-notes.md"
else
  gh release create "$TAG" \
    "$ASSET_PATH" \
    "$ASSET_PATH.sha256" \
    --repo "$REPOSITORY" \
    --title "Jarvis Runtime $TAG" \
    --notes-file "$DIST_DIR/runtime-release-notes.md" \
    $DRAFT_FLAG
fi

echo "Released $TAG to https://github.com/$REPOSITORY/releases/tag/$TAG"

if [[ -n "$DRAFT_FLAG" ]]; then
  echo "Skipping Homebrew formula update for draft release $TAG."
elif [[ "$SKIP_HOMEBREW" == "1" ]]; then
  echo "Skipping Homebrew formula update because SKIP_HOMEBREW=1."
else
  "$ROOT_DIR/scripts/update_homebrew_formula.sh" "$VERSION" "$REPOSITORY"
fi
