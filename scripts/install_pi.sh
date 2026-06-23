#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: install_pi.sh

Environment:
  JARVIS_REPO=roughcoder/jarvis          GitHub repository to install from.
  JARVIS_REF=v0.1.21                     Branch, tag, or commit. Defaults to the
                                         current release tag.
  JARVIS_INSTALL_DIR=/opt/jarvis         Install directory.
  JARVIS_DEVICE_ID=room-pi               Device id for this Pi.
  JARVIS_BRAIN_HOST=imac.example         Brain hostname on the private network.
  JARVIS_BRAIN_PORT=8700                 Brain WebSocket port.
  JARVIS_INTERCOM_TOKEN=...              Token issued by the brain.
  JARVIS_UV_BIN=/usr/local/bin/uv         uv binary used by installed helpers.
  JARVIS_PYTHON_BIN=python3               Python used by uv on Raspberry Pi OS.
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
REF="${JARVIS_REF:-v0.1.21}"
INSTALL_DIR="${JARVIS_INSTALL_DIR:-/opt/jarvis}"
DEVICE_ID="${JARVIS_DEVICE_ID:-room-pi}"
BRAIN_HOST="${JARVIS_BRAIN_HOST:-}"
BRAIN_PORT="${JARVIS_BRAIN_PORT:-8700}"
INTERCOM_TOKEN="${JARVIS_INTERCOM_TOKEN:-}"
UV_BIN="${JARVIS_UV_BIN:-/usr/local/bin/uv}"
PYTHON_BIN="${JARVIS_PYTHON_BIN:-python3}"

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
  rsync \
  python3 \
  python3-venv \
  build-essential \
  portaudio19-dev \
  libasound2-dev \
  alsa-utils

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
  run env UV_PYTHON="$PYTHON_BIN" UV_LINK_MODE=copy uv sync --no-dev --extra stt --extra vad-lite --extra wake
else
  cd "$INSTALL_DIR"
  env UV_PYTHON="$PYTHON_BIN" UV_LINK_MODE=copy uv sync --no-dev --extra stt --extra vad-lite --extra wake
fi

if [[ "$DRY_RUN" == "1" ]]; then
  echo "+ write $INSTALL_DIR/.env"
  echo "+ set VAD_ENGINE=webrtc"
else
  cat > "$INSTALL_DIR/.env" <<ENV
INTERCOM_BRAIN_HOST=$BRAIN_HOST
INTERCOM_BRAIN_PORT=$BRAIN_PORT
INTERCOM_TOKEN=$INTERCOM_TOKEN
CAPS_DEVICE_ID=$DEVICE_ID
CAPS_IDENTITY=house
CAPS_SCOPE=house
VAD_ENGINE=webrtc
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
export UV_PYTHON="$PYTHON_BIN"
exec "$UV_BIN" run jarvis "\$@"
EOF
fi
run chmod 0755 /usr/local/bin/jarvis

if [[ "$DRY_RUN" == "1" ]]; then
  echo "+ write /usr/local/bin/jarvis-pi"
else
  cat > /usr/local/bin/jarvis-pi <<EOF
#!/usr/bin/env bash
set -euo pipefail

INSTALL_DIR="$INSTALL_DIR"
REPO="$REPO"
REF="$REF"
SERVICE="jarvis-intercom.service"
UV_BIN="\${JARVIS_UV_BIN:-$UV_BIN}"
PYTHON_BIN="\${JARVIS_PYTHON_BIN:-$PYTHON_BIN}"

usage() {
  cat <<'PISCRIPT_USAGE'
Usage: jarvis-pi <command>

Commands:
  update    Refresh the installed runtime, sync dependencies, and restart intercom
  restart   Restart the intercom service
  status    Show systemd service status
  logs      Follow intercom service logs
  doctor    Print basic Pi audio/camera/service readiness
PISCRIPT_USAGE
}

require_root() {
  if [[ "\$(id -u)" -ne 0 ]]; then
    echo "Run as root: sudo jarvis-pi \$1" >&2
    exit 1
  fi
}

doctor() {
  echo "Jarvis Pi doctor"
  echo "install_dir: \$INSTALL_DIR"
  if [[ -r "\$INSTALL_DIR/.env" ]]; then
    grep -E '^(CAPS_DEVICE_ID|INTERCOM_BRAIN_HOST|INTERCOM_BRAIN_PORT)=' "\$INSTALL_DIR/.env" || true
  else
    echo "env: missing or unreadable \$INSTALL_DIR/.env"
  fi

  if command -v "\$UV_BIN" >/dev/null 2>&1; then
    "\$UV_BIN" --version
  else
    echo "uv: missing at \$UV_BIN"
  fi

  systemctl is-enabled "\$SERVICE" 2>/dev/null || true
  systemctl is-active "\$SERVICE" 2>/dev/null || true

  if command -v arecord >/dev/null 2>&1; then
    arecord -l || true
  else
    echo "arecord: missing"
  fi

  if command -v aplay >/dev/null 2>&1; then
    aplay -l || true
  else
    echo "aplay: missing"
  fi

  if command -v rpicam-hello >/dev/null 2>&1; then
    rpicam-hello --list-cameras || true
  elif command -v libcamera-hello >/dev/null 2>&1; then
    libcamera-hello --list-cameras || true
  else
    echo "camera: rpicam-hello/libcamera-hello not installed"
  fi

  found_display=0
  if command -v vcgencmd >/dev/null 2>&1; then
    vcgencmd display_power || true
    found_display=1
  fi
  if [[ -e /dev/fb0 ]]; then
    echo "display: framebuffer /dev/fb0 present"
    found_display=1
  fi
  if compgen -G "/dev/dri/card*" >/dev/null; then
    echo "display: DRM devices"
    ls -1 /dev/dri/card* || true
    found_display=1
  fi
  if [[ "\$found_display" -eq 0 ]]; then
    echo "display: no framebuffer or DRM card detected"
  fi
}

cmd="\${1:-}"
case "\$cmd" in
  update)
    require_root "\$cmd"
    tmp_dir="\$(mktemp -d)"
    cleanup() {
      rm -rf "\$tmp_dir"
    }
    trap cleanup EXIT
    archive="\$tmp_dir/jarvis.tar.gz"
    source_dir="\$tmp_dir/source"
    mkdir -p "\$source_dir"
    curl -fsSL "https://github.com/\$REPO/archive/\$REF.tar.gz" -o "\$archive"
    tar -xzf "\$archive" --strip-components=1 -C "\$source_dir"
    rsync -a --delete --exclude .env --exclude .venv --exclude jarvis-workspace "\$source_dir/" "\$INSTALL_DIR/"
    cd "\$INSTALL_DIR"
    env UV_PYTHON="\$PYTHON_BIN" UV_LINK_MODE=copy "\$UV_BIN" sync --no-dev --extra stt --extra vad-lite --extra wake
    systemctl daemon-reload
    systemctl restart "\$SERVICE"
    echo "Jarvis Pi runtime updated and \$SERVICE restarted."
    ;;
  restart)
    require_root "\$cmd"
    systemctl restart "\$SERVICE"
    ;;
  status)
    systemctl status "\$SERVICE"
    ;;
  logs)
    journalctl -u "\$SERVICE" -f --no-pager
    ;;
  doctor)
    doctor
    ;;
  -h|--help|help|"")
    usage
    ;;
  *)
    echo "Unknown command: \$cmd" >&2
    usage >&2
    exit 2
    ;;
esac
EOF
fi
run chmod 0755 /usr/local/bin/jarvis-pi

run jarvis service install intercom \
  --platform systemd \
  --jarvis-bin /usr/local/bin/jarvis \
  --workdir "$INSTALL_DIR"

run systemctl daemon-reload
run systemctl enable --now jarvis-intercom.service

echo "Jarvis Pi intercom installed as $DEVICE_ID."
echo "Check status with: systemctl status jarvis-intercom.service"
echo "Check hardware with: jarvis-pi doctor"
echo "Update later with: sudo jarvis-pi update"
cat <<NEXT

Physical bring-up evidence:
  mkdir -p ~/Desktop/jarvis-bringup-evidence
  jarvis bringup --json --role intercom --platform systemd --hardware \\
    --brain-host $BRAIN_HOST --output ~/Desktop/jarvis-bringup-evidence
NEXT
