#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/compute_next_release_version.sh [base_tag]

Computes the next SemVer by scanning commits since the provided base tag or the
latest v* tag.

Examples:
  scripts/compute_next_release_version.sh
  scripts/compute_next_release_version.sh v0.1.20
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

BASE_TAG="${1:-}"
if [[ -z "$BASE_TAG" ]]; then
  BASE_TAG="$(git tag --list 'v[0-9]*.[0-9]*.[0-9]*' --sort=-v:refname | head -n 1 || true)"
fi

if [[ -z "$BASE_TAG" ]]; then
  BASE_MAJOR=0
  BASE_MINOR=0
  BASE_PATCH=0
else
  BASE_VERSION="${BASE_TAG#v}"
  if ! [[ "$BASE_VERSION" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)(-[0-9A-Za-z.-]+)?$ ]]; then
    echo "Current version tag '$BASE_TAG' is not semver-compatible." >&2
    exit 2
  fi

  BASE_MAJOR="${BASH_REMATCH[1]}"
  BASE_MINOR="${BASH_REMATCH[2]}"
  BASE_PATCH="${BASH_REMATCH[3]}"
fi

range="HEAD"
if [[ -n "$BASE_TAG" ]]; then
  range="$BASE_TAG..HEAD"
fi

commits=( $(git log --no-merges --format=%H "$range") )
if [[ ${#commits[@]} -eq 0 ]]; then
  echo "No commits found for release. Add conventional commits first." >&2
  exit 2
fi

bump_major=0
bump_minor=0
bump_patch=0
ignore_non_conventional="${JARVIS_IGNORE_NON_CONVENTIONAL_COMMITS:-1}"

for commit in "${commits[@]}"; do
  body="$(git log --format=%B -n 1 "$commit")"
  subject="${body%%$'\n'*}"

  if ! printf '%s\n' "$subject" | grep -Eq '^[a-z]+(\([^)]+\))?(!)?:[[:space:]]+.+$'; then
    if [[ "$ignore_non_conventional" == "1" ]]; then
      echo "Ignoring legacy/non-conventional commit in version computation: $commit" >&2
      echo "  $subject" >&2
      echo "(consider replacing this commit message with Conventional Commit format)" >&2
      continue
    fi

    echo "Invalid commit message format: $commit" >&2
    echo "  $subject" >&2
    echo "Expected conventional commit format: type(scope)?: subject" >&2
    exit 3
  fi

  type="$(printf '%s' "$subject" | sed -E 's/^([a-z]+)(\([^)]+\))?(!)?:[[:space:]]+.+$/\1/')"
  bang=""
  if printf '%s\n' "$subject" | grep -Eq '^[a-z]+(\([^)]+\))?!:[[:space:]]+.+$'; then
    bang="!"
  fi
  breaking=""
  if printf '%s\n' "$body" | grep -Eq '^BREAKING CHANGE:' ; then
    breaking=1
  fi

  if [[ -n "$bang" || -n "$breaking" ]]; then
    bump_major=1
  fi

  case "$type" in
    feat)
      bump_minor=1
      ;;
    fix|chore|docs|style|refactor|test|perf|build|ci|revert)
      bump_patch=1
      ;;
    *)
      echo "Invalid commit type '$type' in $commit" >&2
      echo "  $subject" >&2
      echo "Allowed types: feat, fix, chore, docs, style, refactor, test, perf, build, ci, revert" >&2
      exit 3
      ;;
  esac

done

if [[ $bump_major -eq 1 ]]; then
  BASE_MAJOR=$((BASE_MAJOR + 1))
  BASE_MINOR=0
  BASE_PATCH=0
elif [[ $bump_minor -eq 1 ]]; then
  BASE_MINOR=$((BASE_MINOR + 1))
  BASE_PATCH=0
elif [[ $bump_patch -eq 1 ]]; then
  BASE_PATCH=$((BASE_PATCH + 1))
else
  echo "No releasable conventional commit types found since '$BASE_TAG'." >&2
  exit 2
fi

echo "${BASE_MAJOR}.${BASE_MINOR}.${BASE_PATCH}"
