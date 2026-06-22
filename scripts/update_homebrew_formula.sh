#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/update_homebrew_formula.sh <version> [repository]

Updates, validates, commits, and pushes the Jarvis Homebrew formula after a
runtime GitHub Release exists.

Environment:
  HOMEBREW_TAP_DIR=/path/to/homebrew-infinite-stack
  HOMEBREW_TAP_NAME=roughcoder/infinite-stack
  HOMEBREW_FORMULA_TOKEN=jarvis

Example:
  scripts/update_homebrew_formula.sh 0.1.0 roughcoder/jarvis
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

VERSION="${1:-${VERSION:-}}"
REPOSITORY="${2:-${GITHUB_REPOSITORY:-roughcoder/jarvis}}"
if [[ -z "$VERSION" ]]; then
  usage >&2
  exit 2
fi

VERSION="${VERSION#v}"
TAG="v$VERSION"
ASSET_NAME="${JARVIS_RUNTIME_ASSET_NAME:-jarvis-$VERSION.tar.gz}"
TAP_DIR="${HOMEBREW_TAP_DIR:-$HOME/Development/homebrew-infinite-stack}"
TAP_NAME="${HOMEBREW_TAP_NAME:-roughcoder/infinite-stack}"
FORMULA_TOKEN="${HOMEBREW_FORMULA_TOKEN:-jarvis}"
FORMULA_RELATIVE_PATH="Formula/$FORMULA_TOKEN.rb"
FORMULA_FILE="$TAP_DIR/$FORMULA_RELATIVE_PATH"

if ! command -v gh >/dev/null 2>&1; then
  echo "GitHub CLI is required: brew install gh" >&2
  exit 1
fi

if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew is required to validate the formula." >&2
  exit 1
fi

gh auth status >/dev/null

if [[ ! -d "$TAP_DIR/.git" ]]; then
  echo "Homebrew tap checkout not found: $TAP_DIR" >&2
  exit 1
fi

if [[ ! -f "$FORMULA_FILE" ]]; then
  echo "Formula file not found: $FORMULA_FILE" >&2
  exit 1
fi

if [[ -n "$(git -C "$TAP_DIR" status --porcelain)" ]]; then
  echo "Homebrew tap has uncommitted changes. Commit or stash before updating:" >&2
  git -C "$TAP_DIR" status --short
  exit 1
fi

git -C "$TAP_DIR" pull --ff-only

ASSET_EXISTS="$(
  gh release view "$TAG" \
    --repo "$REPOSITORY" \
    --json assets \
    -q ".assets[] | select(.name == \"$ASSET_NAME\") | .name"
)"

if [[ -z "$ASSET_EXISTS" ]]; then
  echo "Release $TAG in $REPOSITORY does not include $ASSET_NAME." >&2
  exit 1
fi

SHA256="$(
  gh release download "$TAG" \
    --repo "$REPOSITORY" \
    --pattern "$ASSET_NAME.sha256" \
    --output - \
    2>/dev/null \
    | awk '{print $1}'
)"

if [[ ! "$SHA256" =~ ^[0-9a-f]{64}$ ]]; then
  echo "Could not read a valid SHA-256 for $ASSET_NAME from $TAG." >&2
  exit 1
fi

FORMULA_FILE="$FORMULA_FILE" VERSION="$VERSION" SHA256="$SHA256" REPOSITORY="$REPOSITORY" ASSET_NAME="$ASSET_NAME" ruby <<'RUBY'
path = ENV.fetch("FORMULA_FILE")
version = ENV.fetch("VERSION")
sha256 = ENV.fetch("SHA256")
repository = ENV.fetch("REPOSITORY")
asset_name = ENV.fetch("ASSET_NAME")
public_url = "https://github.com/#{repository}/releases/download/v#{version}/#{asset_name}"

text = File.read(path)

if text.match?(/^  url "/)
  text.sub!(/^  url "[^"]+"$/, %(  url "#{public_url}"))
else
  text.sub!(/^  homepage "[^"]+"$/, %(  homepage "https://github.com/#{repository}"\n  url "#{public_url}"))
end

if text.match?(/^  sha256 "/)
  text.sub!(/^  sha256 "[0-9a-f]{64}"$/, %(  sha256 "#{sha256}"))
else
  text.sub!(/^  url "[^"]+"$/, %(  url "#{public_url}"\n  sha256 "#{sha256}"))
end

text.sub!(
  /      The formula currently tracks HEAD while the runtime public release and\n      versioned tarball flow are being prepared\.\n/,
  ""
)

File.write(path, text)
RUBY

FORMULA_CHANGED=0
if ! git -C "$TAP_DIR" diff --quiet -- "$FORMULA_RELATIVE_PATH"; then
  FORMULA_CHANGED=1
  git -C "$TAP_DIR" add "$FORMULA_RELATIVE_PATH"
  git -C "$TAP_DIR" commit -m "Update Jarvis runtime formula to $VERSION

Constraint: public runtime installs should use a versioned release tarball instead of HEAD-only source
Rejected: keep jarvis formula HEAD-only | it weakens update predictability for fresh fleet installs
Confidence: high
Scope-risk: narrow
Directive: update version URL and sha256 together for every Jarvis runtime release
Tested: brew style --formula $TAP_NAME/$FORMULA_TOKEN; brew audit --formula $TAP_NAME/$FORMULA_TOKEN; brew fetch --formula --force $TAP_NAME/$FORMULA_TOKEN
Not-tested: brew install $FORMULA_TOKEN on a clean Mac"
else
  echo "$FORMULA_TOKEN is already up to date for $TAG."
fi

if ! brew --repo "$TAP_NAME" >/dev/null 2>&1; then
  brew tap "$TAP_NAME" "$TAP_DIR" --custom-remote
fi

BREW_TAP_REPO="$(brew --repo "$TAP_NAME")"
BREW_TAP_REMOTE="$(git -C "$BREW_TAP_REPO" remote get-url origin 2>/dev/null || true)"
if [[ "$BREW_TAP_REMOTE" != "$TAP_DIR" ]]; then
  git -C "$BREW_TAP_REPO" remote set-url origin "$TAP_DIR"
fi
git -C "$BREW_TAP_REPO" pull --ff-only

brew style --formula "$TAP_NAME/$FORMULA_TOKEN"
brew audit --formula "$TAP_NAME/$FORMULA_TOKEN"
brew fetch --formula --force "$TAP_NAME/$FORMULA_TOKEN"

if [[ "$FORMULA_CHANGED" -eq 1 ]]; then
  CURRENT_BRANCH="$(git -C "$TAP_DIR" branch --show-current)"
  git -C "$TAP_DIR" push origin "$CURRENT_BRANCH"
  echo "Updated $TAP_NAME/$FORMULA_TOKEN to $TAG."
else
  echo "Validated $TAP_NAME/$FORMULA_TOKEN for $TAG."
fi

