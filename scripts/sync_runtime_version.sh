#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/sync_runtime_version.sh <version> [--commit]

Synchronizes runtime version metadata (pyproject.toml, src/jarvis/__init__.py,
uv.lock) with the release version. Optionally commits the changes.

Usage with commit:
  scripts/sync_runtime_version.sh 0.1.23 --commit
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

COMMIT=0
if [[ "${2:-}" == "--commit" ]]; then
  COMMIT=1
elif [[ -n "${2:-}" ]]; then
  echo "Unexpected argument: ${2}" >&2
  usage >&2
  exit 2
fi

if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$ ]]; then
  echo "Version must look like 1.2.3, optionally with prerelease metadata." >&2
  exit 2
fi

PYPROJECT_VERSION="$(python3 - <<'PY'
import tomllib
from pathlib import Path
print(tomllib.loads(Path('pyproject.toml').read_text(encoding='utf-8'))['project']['version'])
PY
)"
INIT_VERSION="$(python3 - <<'PY'
import ast
from pathlib import Path
path = Path('src/jarvis/__init__.py')
for node in ast.parse(path.read_text(encoding='utf-8')).body:
    if isinstance(node, ast.Assign):
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == '__version__':
                print(ast.literal_eval(node.value))
                raise SystemExit
raise SystemExit('__version__ not found')
PY
)"

if [[ "$PYPROJECT_VERSION" == "$VERSION" && "$INIT_VERSION" == "$VERSION" ]]; then
  echo "Runtime version metadata already at $VERSION."
  if [[ "$COMMIT" -eq 0 ]]; then
    exit 0
  fi
fi

python3 - "$VERSION" <<'PY'
from pathlib import Path
import re
import sys

version = sys.argv[1]

def replace_first(path, pattern):
    data = Path(path).read_text(encoding='utf-8')
    new_data, count = re.subn(pattern, lambda m: f'{m.group(1)}"{version}"', data, count=1, flags=re.MULTILINE)
    if count != 1:
        raise SystemExit(f'Failed to update {path}')
    Path(path).write_text(new_data, encoding='utf-8')

replace_first('pyproject.toml', r'(^version\s*=\s*)"[^"]+"')
replace_first('src/jarvis/__init__.py', r'(^__version__\s*=\s*)"[^"]+"')
PY

if [[ "$COMMIT" -eq 1 ]]; then
  if [[ -n "$(git status --porcelain pyproject.toml src/jarvis/__init__.py)" ]]; then
    git add pyproject.toml src/jarvis/__init__.py
    git commit -m "chore(version): sync runtime metadata to $VERSION"
    echo "Committed runtime version bump to $VERSION."
  else
    echo "No version metadata changes to commit."
  fi
fi
