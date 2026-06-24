"""Packaged setup helpers for the macOS onboarding app.

The Swift app owns the experience; this module owns the durable runtime shape:
dotenv merging, generated pairing tokens, and existing user-file formats.
"""

from __future__ import annotations

import json
import re
import secrets
import subprocess
from pathlib import Path
from typing import Any

from jarvis.brain.identity import _parse_front_matter
from jarvis.brain.profile import remember_fact
from jarvis.deploy import ROLES


SECRET_KEYS = {
    "ANTHROPIC_API_KEY",
    "GEMINI_API_KEY",
    "GATEWAY_API_KEY",
    "GATEWAY_CLIENT_KEY",
    "HONCHO_LLM_KEY",
    "OPENAI_API_KEY",
    "OPENROUTER_API_KEY",
    "TOOLS_WEBSEARCH_API_KEY",
    "TTS_API_KEY",
    "WHATSAPP_TOKEN",
    "WORKER_PEEKABOO_OPENAI_API_KEY",
    "WORKER_PEEKABOO_OPENROUTER_API_KEY",
    "WORKER_TOKEN",
}


def read_setup(env_file: str | Path) -> dict[str, Any]:
    path = Path(env_file).expanduser()
    values = _read_dotenv(path)
    workdir = path.parent
    users_dir = _resolved_dir(workdir, values.get("CAPS_USERS_DIR", "jarvis-workspace/users"))
    admin = _read_admin(values, users_dir)
    roles = _infer_roles(values)
    return {
        "env_file": str(path),
        "admin": admin,
        "machine": {
            "device_id": values.get("CAPS_DEVICE_ID", "local-mac"),
            "room": values.get("GATEWAY_ROOM", "default"),
            "personal": values.get("CAPS_SCOPE", "house") == "personal",
        },
        "roles": roles,
        "providers": {
            "has_anthropic_api_key": bool(values.get("ANTHROPIC_API_KEY")),
            "has_gemini_api_key": bool(values.get("GEMINI_API_KEY")),
            "has_openai_api_key": bool(values.get("OPENAI_API_KEY")),
            "has_openrouter_api_key": bool(values.get("OPENROUTER_API_KEY")),
            "has_tools_websearch_api_key": bool(values.get("TOOLS_WEBSEARCH_API_KEY")),
            "has_tts_api_key": bool(values.get("TTS_API_KEY")),
            "has_worker_peekaboo_openai_api_key": bool(values.get("WORKER_PEEKABOO_OPENAI_API_KEY")),
            "has_worker_peekaboo_openrouter_api_key": bool(values.get("WORKER_PEEKABOO_OPENROUTER_API_KEY")),
        },
        "brain": {
            "host": values.get("BRAIN_HOST", "localhost"),
            "port": values.get("BRAIN_PORT", "8700"),
        },
        "intercom": {
            "brain_host": values.get("INTERCOM_BRAIN_HOST", "localhost"),
            "brain_port": values.get("INTERCOM_BRAIN_PORT", "8700"),
            "paired": bool(values.get("INTERCOM_TOKEN")),
        },
        "worker": {
            "repo_root": values.get("WORKER_REPO_ROOT", ""),
            "agent": values.get("WORKER_AGENT", "codex"),
            "shell_secrets": values.get("WORKER_SHELL_SECRETS", ""),
            "peekaboo_ai_providers": values.get("WORKER_PEEKABOO_AI_PROVIDERS", ""),
            "peekaboo_openai_base_url": values.get("WORKER_PEEKABOO_OPENAI_BASE_URL", ""),
            "peekaboo_agent_model": values.get("WORKER_PEEKABOO_AGENT_MODEL", "gpt-5.5"),
        },
        "whatsapp": {
            "enabled": _bool(values.get("WHATSAPP_ENABLED", "false")),
            "admin": values.get("WHATSAPP_ADMIN", ""),
            "dm_policy": values.get("WHATSAPP_DM_POLICY", "allowlist"),
            "account": values.get("WHATSAPP_ACCOUNT", ""),
        },
    }


def apply_setup(env_file: str | Path, payload: dict[str, Any]) -> dict[str, Any]:
    path = Path(env_file).expanduser()
    workdir = path.parent
    values = _read_dotenv(path)
    admin = payload.get("admin") or {}
    machine = payload.get("machine") or {}
    providers = payload.get("providers") or {}
    brain = payload.get("brain") or {}
    intercom = payload.get("intercom") or {}
    worker = payload.get("worker") or {}
    whatsapp = payload.get("whatsapp") or {}
    roles = [str(r) for r in payload.get("roles") or [] if str(r) in ROLES]

    admin_slug = _slug(str(admin.get("name") or values.get("CAPS_IDENTITY") or "admin"))
    device_id = _slug(str(machine.get("device_id") or values.get("CAPS_DEVICE_ID") or "local-mac"))
    room = str(machine.get("room") or values.get("GATEWAY_ROOM") or "default").strip() or "default"
    personal = bool(machine.get("personal", True))

    updates: dict[str, str] = {
        "CAPS_DEVICE_ID": device_id,
        "CAPS_IDENTITY": admin_slug if personal else "house",
        "CAPS_SCOPE": "personal" if personal else "house",
        "CAPS_PROFILES_DIR": values.get("CAPS_PROFILES_DIR", "jarvis-workspace/profiles"),
        "CAPS_USERS_DIR": values.get("CAPS_USERS_DIR", "jarvis-workspace/users"),
        "GATEWAY_ROOM": room,
    }

    if "brain" in roles:
        updates |= {
            "BRAIN_HOST": str(brain.get("host") or values.get("BRAIN_HOST") or "0.0.0.0"),
            "BRAIN_PORT": str(brain.get("port") or values.get("BRAIN_PORT") or "8700"),
            "GATEWAY_API_KEY": _existing_or_token(values, "GATEWAY_API_KEY"),
            "GATEWAY_CLIENT_KEY": _existing_or_token(values, "GATEWAY_CLIENT_KEY"),
            "HONCHO_LLM_KEY": _existing_or_token(values, "HONCHO_LLM_KEY"),
        }
    if "intercom" in roles:
        if "brain" in roles:
            token = _device_token(values, device_id)
            updates |= {
                "INTERCOM_BRAIN_HOST": "localhost",
                "INTERCOM_BRAIN_PORT": str(updates.get("BRAIN_PORT") or values.get("BRAIN_PORT") or "8700"),
                "INTERCOM_TOKEN": token,
            }
        else:
            updates |= {
                "INTERCOM_BRAIN_HOST": str(intercom.get("brain_host") or values.get("INTERCOM_BRAIN_HOST") or ""),
                "INTERCOM_BRAIN_PORT": str(intercom.get("brain_port") or values.get("INTERCOM_BRAIN_PORT") or "8700"),
            }
            if intercom.get("token"):
                updates["INTERCOM_TOKEN"] = str(intercom["token"])
    if "worker" in roles:
        updates |= {
            "WORKER_HOST": values.get("WORKER_HOST", "localhost"),
            "WORKER_BIND_HOST": values.get("WORKER_BIND_HOST", "localhost"),
            "WORKER_TOKEN": _existing_or_token(values, "WORKER_TOKEN"),
            "WORKER_REPO_ROOT": str(worker.get("repo_root") or values.get("WORKER_REPO_ROOT") or ""),
            "WORKER_AGENT": str(worker.get("agent") or values.get("WORKER_AGENT") or "codex"),
            "WORKER_SHELL_SECRETS": str(worker.get("shell_secrets") or values.get("WORKER_SHELL_SECRETS") or ""),
            "WORKER_PEEKABOO_AI_PROVIDERS": str(worker.get("peekaboo_ai_providers") or values.get("WORKER_PEEKABOO_AI_PROVIDERS") or ""),
            "WORKER_PEEKABOO_OPENAI_BASE_URL": str(worker.get("peekaboo_openai_base_url") or values.get("WORKER_PEEKABOO_OPENAI_BASE_URL") or ""),
            "WORKER_PEEKABOO_AGENT_MODEL": str(worker.get("peekaboo_agent_model") or values.get("WORKER_PEEKABOO_AGENT_MODEL") or "gpt-5.5"),
        }
    if "whatsapp" in roles:
        wa_token = _device_token(values, str(whatsapp.get("device_id") or values.get("WHATSAPP_DEVICE_ID") or "whatsapp"))
        updates |= {
            "WHATSAPP_ENABLED": "true",
            "WHATSAPP_DEVICE_ID": str(whatsapp.get("device_id") or values.get("WHATSAPP_DEVICE_ID") or "whatsapp"),
            "WHATSAPP_TOKEN": wa_token,
            "WHATSAPP_DM_POLICY": str(whatsapp.get("dm_policy") or "pairing"),
            "WHATSAPP_ADMIN": _digits(str(whatsapp.get("admin") or admin.get("whatsapp_admin") or admin.get("phone") or "")),
            "WHATSAPP_ACCOUNT": str(whatsapp.get("account") or values.get("WHATSAPP_ACCOUNT") or ""),
        }

    provider_map = {
        "anthropic_api_key": "ANTHROPIC_API_KEY",
        "gemini_api_key": "GEMINI_API_KEY",
        "openai_api_key": "OPENAI_API_KEY",
        "openrouter_api_key": "OPENROUTER_API_KEY",
        "tools_websearch_api_key": "TOOLS_WEBSEARCH_API_KEY",
        "tts_api_key": "TTS_API_KEY",
        "worker_peekaboo_openai_api_key": "WORKER_PEEKABOO_OPENAI_API_KEY",
        "worker_peekaboo_openrouter_api_key": "WORKER_PEEKABOO_OPENROUTER_API_KEY",
    }
    for src, dest in provider_map.items():
        value = str(providers.get(src) or "").strip()
        if value:
            updates[dest] = value

    devices = _read_brain_devices(values)
    if "brain" in roles and "intercom" in roles:
        devices = _upsert_device(devices, device_id, updates["INTERCOM_TOKEN"], admin_slug if personal else "")
    if "brain" in roles and "whatsapp" in roles:
        devices = _upsert_device(devices, updates["WHATSAPP_DEVICE_ID"], updates["WHATSAPP_TOKEN"], "")
    if devices:
        updates["BRAIN_DEVICES"] = json.dumps(devices, separators=(",", ":"))

    merged = {**values, **updates}
    _write_dotenv(path, updates)
    user_path = _write_admin_user(workdir, merged, admin_slug, admin, device_id)
    return {
        "env_file": str(path),
        "user_file": str(user_path),
        "roles": roles,
        "changed_keys": sorted(updates),
    }


def validate_setup(env_file: str | Path, roles: list[str]) -> dict[str, Any]:
    values = _read_dotenv(Path(env_file).expanduser())
    missing: list[str] = []
    warnings: list[str] = []
    selected = set(roles)
    if "brain" in selected:
        if not (values.get("OPENAI_API_KEY") or values.get("OPENROUTER_API_KEY") or values.get("ANTHROPIC_API_KEY")):
            missing.append("one LLM provider key")
        if not values.get("TTS_API_KEY"):
            warnings.append("TTS_API_KEY is empty; spoken replies may fail")
        if not values.get("BRAIN_DEVICES"):
            warnings.append("BRAIN_DEVICES has no paired devices")
    if "intercom" in selected and not values.get("INTERCOM_TOKEN"):
        missing.append("INTERCOM_TOKEN")
    if "worker" in selected and not values.get("WORKER_TOKEN"):
        missing.append("WORKER_TOKEN")
    if "whatsapp" in selected:
        if not values.get("WHATSAPP_TOKEN"):
            missing.append("WHATSAPP_TOKEN")
        if not values.get("WHATSAPP_ADMIN"):
            warnings.append("WHATSAPP_ADMIN is empty; new WhatsApp users cannot be approved")
    return {"ok": not missing, "missing": missing, "warnings": warnings}


def whatsapp_auth(*, wacli_bin: str = "wacli", account: str = "") -> dict[str, Any]:
    argv = [wacli_bin]
    if account:
        argv += ["--account", account]
    argv += ["auth", "--qr-format", "text"]
    try:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=300, check=False)
    except FileNotFoundError:
        return {"ok": False, "argv": argv, "returncode": 127, "stdout": "", "stderr": "wacli not found"}
    except subprocess.TimeoutExpired as exc:
        return {
            "ok": False,
            "argv": argv,
            "returncode": 124,
            "stdout": exc.stdout or "",
            "stderr": exc.stderr or "wacli auth timed out",
        }
    return {
        "ok": result.returncode == 0,
        "argv": argv,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def _read_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = _unquote(value.strip())
    return values


def _write_dotenv(path: Path, updates: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True) if path.exists() else []
    remaining = dict(updates)
    out: list[str] = []
    for line in lines:
        stripped = line.lstrip()
        key = stripped.split("=", 1)[0].strip() if "=" in stripped and not stripped.startswith("#") else ""
        if key in remaining:
            out.append(f"{key}={_quote(remaining.pop(key))}\n")
        else:
            out.append(line)
    if out and not out[-1].endswith(("\n", "\r")):
        out[-1] += "\n"
    for key in sorted(remaining):
        out.append(f"{key}={_quote(remaining[key])}\n")
    path.write_text("".join(out), encoding="utf-8")
    path.chmod(0o600)


def _write_admin_user(workdir: Path, values: dict[str, str], admin_slug: str, admin: dict[str, Any], device_id: str) -> Path:
    users_dir = _resolved_dir(workdir, values.get("CAPS_USERS_DIR", "jarvis-workspace/users"))
    users_dir.mkdir(parents=True, exist_ok=True)
    path = users_dir / f"{admin_slug}.md"
    text = path.read_text(encoding="utf-8", errors="replace") if path.exists() else f"# {admin.get('name') or admin_slug}\n"
    fm = _parse_front_matter(text)
    devices = _as_list(fm.get("devices"))
    if device_id not in devices:
        devices.append(device_id)
    whatsapp = _as_list(fm.get("whatsapp"))
    phone = _digits(str(admin.get("whatsapp_admin") or admin.get("phone") or ""))
    if phone and phone not in [_digits(w) for w in whatsapp]:
        whatsapp.append(phone)
    lines = [
        "---",
        f"devices: [{', '.join(json.dumps(v) for v in devices)}]",
        f"whatsapp: [{', '.join(json.dumps(v) for v in whatsapp)}]",
        "scope: personal",
        f"honcho_peer: {admin_slug}",
        "capabilities: [profile.write]",
        "---",
        "",
        f"# {admin.get('name') or admin_slug}",
        "",
    ]
    body = re.sub(r"^\s*---\s*\n.*?\n---\s*(?:\n|$)", "", text, count=1, flags=re.DOTALL)
    body = re.sub(rf"^#\s+{re.escape(path.stem)}\s*\n*", "", body).strip()
    path.write_text("\n".join(lines) + (body + "\n" if body else ""), encoding="utf-8")
    if admin.get("email"):
        remember_fact(path, "email", str(admin["email"]))
    if admin.get("phone"):
        remember_fact(path, "phone", str(admin["phone"]))
    return path


def _read_admin(values: dict[str, str], users_dir: Path) -> dict[str, Any]:
    identity = values.get("CAPS_IDENTITY", "")
    if not identity or identity == "house":
        return {"name": "", "email": "", "phone": "", "whatsapp_admin": values.get("WHATSAPP_ADMIN", "")}
    path = users_dir / f"{identity}.md"
    out = {"name": identity, "email": "", "phone": "", "whatsapp_admin": values.get("WHATSAPP_ADMIN", "")}
    if not path.is_file():
        return out
    text = path.read_text(encoding="utf-8", errors="replace")
    title = re.search(r"^#\s+(.+)$", text, re.MULTILINE)
    if title:
        out["name"] = title.group(1).strip()
    facts = dict(re.findall(r"^- ([^:]+):\s*(.*)$", text, flags=re.MULTILINE))
    out["email"] = facts.get("email", "")
    out["phone"] = facts.get("phone", "")
    fm = _parse_front_matter(text)
    whatsapps = _as_list(fm.get("whatsapp"))
    if whatsapps and not out["whatsapp_admin"]:
        out["whatsapp_admin"] = whatsapps[0]
    return out


def _infer_roles(values: dict[str, str]) -> list[str]:
    roles: list[str] = []
    if values.get("BRAIN_HOST"):
        roles.append("brain")
    if values.get("INTERCOM_TOKEN") or values.get("INTERCOM_BRAIN_HOST"):
        roles.append("intercom")
    if values.get("WORKER_TOKEN") or values.get("WORKER_REPO_ROOT"):
        roles.append("worker")
    if _bool(values.get("WHATSAPP_ENABLED", "")):
        roles.append("whatsapp")
    return roles


def _read_brain_devices(values: dict[str, str]) -> list[dict[str, str]]:
    try:
        raw = json.loads(values.get("BRAIN_DEVICES", "[]") or "[]")
    except json.JSONDecodeError:
        return []
    return [d for d in raw if isinstance(d, dict)]


def _upsert_device(devices: list[dict[str, str]], device_id: str, token: str, identity: str = "") -> list[dict[str, str]]:
    out = [d for d in devices if str(d.get("device_id", "")) != device_id]
    entry = {"token": token, "device_id": device_id}
    if identity:
        entry["identity"] = identity
    out.append(entry)
    return out


def _device_token(values: dict[str, str], device_id: str) -> str:
    for device in _read_brain_devices(values):
        if device.get("device_id") == device_id and device.get("token"):
            return str(device["token"])
    return secrets.token_urlsafe(32)


def _existing_or_token(values: dict[str, str], key: str) -> str:
    return values.get(key) or f"sk-{secrets.token_urlsafe(24)}"


def _resolved_dir(workdir: Path, value: str) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else workdir / path


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9_-]+", "-", value.strip().lower()).strip("-")
    return slug or "local-mac"


def _digits(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def _bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _as_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value if str(v)]
    if value:
        return [str(value)]
    return []


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    return value.replace('\\"', '"').replace("\\\\", "\\")


def _quote(value: str) -> str:
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'
