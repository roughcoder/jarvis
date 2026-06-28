#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/check_conventional_commits.sh <base-ref> [head-ref]

Checks commit messages in <base-ref>..<head-ref> for Conventional Commits format.

Examples:
  scripts/check_conventional_commits.sh v0.1.21 HEAD
  scripts/check_conventional_commits.sh 123abc4 HEAD
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage >&2
  exit 2
fi

BASE_REF="$1"
HEAD_REF="${2:-HEAD}"

commits=( $(git log --no-merges --format=%H "$BASE_REF..$HEAD_REF") )
if [[ ${#commits[@]} -eq 0 ]]; then
  echo "No commits to check in $BASE_REF..$HEAD_REF"
  exit 0
fi

for commit in "${commits[@]}"; do
  body="$(git log --format=%B -n 1 "$commit")"
  subject="${body%%$'\n'*}"

  if ! printf '%s\n' "$subject" | grep -Eq '^[a-z]+(\([^)]+\))?(!)?:[[:space:]]+.+$'; then
    echo "Invalid commit message format: $commit" >&2
    echo "  $subject" >&2
    echo "Expected conventional commit format: type(scope)?: subject" >&2
    echo "Optional release trailers: Release-note:, Env:, Breaking Change:" >&2
    exit 3
  fi

  type="$(printf '%s' "$subject" | sed -E 's/^([a-z]+)(\([^)]+\))?(!)?:[[:space:]]+.+$/\1/')"
  case "$type" in
    feat|fix|chore|docs|style|refactor|test|perf|build|ci|revert)
      ;;
    *)
      echo "Invalid commit type '$type' in $commit" >&2
      echo "  $subject" >&2
      echo "Allowed types: feat, fix, chore, docs, style, refactor, test, perf, build, ci, revert" >&2
      echo "Optional release trailers: Release-note:, Env:, Breaking Change:" >&2
      exit 3
      ;;
  esac
done

echo "Conventional commit check passed for $BASE_REF..$HEAD_REF"
