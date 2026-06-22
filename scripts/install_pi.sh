#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: install_pi.sh

Environment:
  JARVIS_REPO=roughcoder/jarvis          GitHub repository to install from.
  JARVIS_REF=main                        Branch, tag, or commit.
  JARVIS_INSTALL_DIR=/opt/jarvis         Install directory.
  JARVIS_DEVICE_ID=room-pi               Device id for this Pi.
  JARVIS_BRAIN_HOST=imac.example         Brain hostname on the private network.
  JARVIS_BRAIN_PORT=8700                 Brain WebSocket port.
  JARVIS_INTERCOM_TOKEN=...              Token issued by the brain.
  JARVIS_DRY_RUN=0                       Print commands instead of running.
  JARVIS_DRY_RUN_UV_INSTALLED=0          Dry-run uv install state.
  JARVIS_DRY_RUN_TMP_DIR=/tmp/jarvis-pi  Dry-run temporary directory.

This installer is for Raspberry Pi intercom devices. It installs the thin
intercom runtime only: wake word, VAD, microphone, and speaker. It does not
install brain, worker, LLM, memory, or provider credentials.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

DRY_RUN="${JARVIS_DRY_RUN:-0}"
DRY_RUN_UV_INSTALLED="${JARVIS_DRY_RUN_UV_INSTALLED:-0}"

run() {
  if [[ "$DRY_RUN" == "1" ]]; then
    printf '+'
    printf ' %q' "$@"
    printf '\n'
  else
    "$@"
  fi
}

if [[ "$DRY_RUN" != "1" && "$(id -u)" -ne 0 ]]; then
  echo "Run as root: curl ... | sudo -E bash" >&2
  exit 1
fi

REPO="${JARVIS_REPO:-roughcoder/jarvis}"
REF="${JARVIS_REF:-main}"
INSTALL_DIR="${JARVIS_INSTALL_DIR:-/opt/jarvis}"
DEVICE_ID="${JARVIS_DEVICE_ID:-room-pi}"
BRAIN_HOST="${JARVIS_BRAIN_HOST:-}"
BRAIN_PORT="${JARVIS_BRAIN_PORT:-8700}"
INTERCOM_TOKEN="${JARVIS_INTERCOM_TOKEN:-}"

if [[ -z "$BRAIN_HOST" || -z "$INTERCOM_TOKEN" ]]; then
  echo "Set JARVIS_BRAIN_HOST and JARVIS_INTERCOM_TOKEN before installing." >&2
  exit 2
fi

export DEBIAN_FRONTEND=noninteractive
run apt-get update
run apt-get install -y --no-install-recommends \
  ca-certificates \
  curl \
  tar \
  python3 \
  python3-venv \
  build-essential \
  portaudio19-dev \
  libasound2-dev

if [[ "$DRY_RUN" == "1" ]]; then
  if [[ "$DRY_RUN_UV_INSTALLED" != "1" ]]; then
    run env UV_INSTALL_DIR=/usr/local/bin sh -c "curl -LsSf https://astral.sh/uv/install.sh | sh"
  fi
elif ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh
fi

if [[ "$DRY_RUN" == "1" ]]; then
  tmp_dir="${JARVIS_DRY_RUN_TMP_DIR:-/tmp/jarvis-pi-dry-run}"
else
  tmp_dir="$(mktemp -d)"
  cleanup() {
    rm -rf "$tmp_dir"
  }
  trap cleanup EXIT
fi

archive="$tmp_dir/jarvis.tar.gz"
run curl -fsSL "https://github.com/$REPO/archive/$REF.tar.gz" -o "$archive"
run mkdir -p "$INSTALL_DIR"
run tar -xzf "$archive" --strip-components=1 -C "$INSTALL_DIR"

if [[ "$DRY_RUN" == "1" ]]; then
  echo "+ cd $INSTALL_DIR"
  run uv sync --no-dev --extra stt --extra vad --extra wake
else
  cd "$INSTALL_DIR"
  uv sync --no-dev --extra stt --extra vad --extra wake
fi

if [[ "$DRY_RUN" == "1" ]]; then
  echo "+ write $INSTALL_DIR/.env"
else
  cat > "$INSTALL_DIR/.env" <<ENV
INTERCOM_BRAIN_HOST=$BRAIN_HOST
INTERCOM_BRAIN_PORT=$BRAIN_PORT
INTERCOM_TOKEN=$INTERCOM_TOKEN
CAPS_DEVICE_ID=$DEVICE_ID
CAPS_IDENTITY=house
CAPS_SCOPE=house
ENV
fi
run chmod 0600 "$INSTALL_DIR/.env"

if [[ "$DRY_RUN" == "1" ]]; then
  echo "+ write /usr/local/bin/jarvis"
else
  cat > /usr/local/bin/jarvis <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$INSTALL_DIR"
exec /usr/local/bin/uv run jarvis "\$@"
EOF
fi
run chmod 0755 /usr/local/bin/jarvis

run jarvis service install intercom \
  --platform systemd \
  --jarvis-bin /usr/local/bin/jarvis \
  --workdir "$INSTALL_DIR"

run systemctl daemon-reload
run systemctl enable --now jarvis-intercom.service

echo "Jarvis Pi intercom installed as $DEVICE_ID."
echo "Check status with: systemctl status jarvis-intercom.service"
