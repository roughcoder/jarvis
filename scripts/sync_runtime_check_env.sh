#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

uv sync --dev \
  --extra gateway \
  --extra tts \
  --extra stt \
  --extra vad \
  --extra vad-lite \
  --extra wake \
  --extra memory \
  --extra worker \
  --extra mcp \
  --extra browser
