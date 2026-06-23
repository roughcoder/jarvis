#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Run this script from inside the jarvis repository." >&2
  exit 1
fi

git config core.hooksPath .githooks
chmod +x .githooks/commit-msg

echo "Configured local commit-msg hook from .githooks/commit-msg"
echo "Enforced format: feat|fix|chore|docs|style|refactor|test|perf|build|ci|revert" \
  "(scope optional): message"
