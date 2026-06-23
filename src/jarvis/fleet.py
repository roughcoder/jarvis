"""Fleet status helpers for operator UIs.

The Swift menu bar app should stay a thin shell: poll this JSON contract, show
status, and ask launchd / Jarvis CLI commands to act. It must not reimplement
brain, worker, or capability logic.
"""

from __future__ import annotations

import asyncio
import json
import os
import platform
import subprocess
from pathlib import Path
from typing import Any

import httpx
import websockets

from jarvis import __version__
from jarvis.config import Config
from jarvis.protocol.messages import Hello, Reject, Welcome, decode, encode

SERVICE_LABELS = {
    "brain": "com.jarvis.brain",
    "intercom": "com.jarvis.intercom",
    "worker": "com.jarvis.worker",
}


def _run(argv: list[str], *, cwd: str | None = None, timeout: float = 2.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _short_error(exc: BaseException) -> str:
    text = str(exc).strip()
    return text or exc.__class__.__name__


async def probe_brain(cfg: Config, *, timeout_s: float = 5.0) -> dict[str, Any]:
    """Pair like an intercom and return the resolved identity/capabilities."""
    url = cfg.intercom.brain_url
    try:
        async with websockets.connect(url, open_timeout=timeout_s) as ws:
            await ws.send(
                encode(
                    Hello(
                        device_id=cfg.capabilities.device_id,
                        token=cfg.intercom.token.get_secret_value(),
                    )
                )
            )
            res = decode(await asyncio.wait_for(ws.recv(), timeout_s))
    except Exception as exc:  # noqa: BLE001
        return {"reachable": False, "paired": False, "error": _short_error(exc)}

    if isinstance(res, Welcome):
        return {
            "reachable": True,
            "paired": True,
            "identity": res.identity,
            "scope": res.scope,
            "capabilities": res.capabilities,
        }
    if isinstance(res, Reject):
        return {"reachable": True, "paired": False, "error": res.reason}
    return {"reachable": True, "paired": False, "error": f"unexpected reply: {res}"}


async def probe_worker(cfg: Config, *, timeout_s: float = 5.0) -> dict[str, Any]:
    """Probe the configured worker daemon without exposing its bearer token."""
    token = cfg.worker.token.get_secret_value()
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            health = await client.get(f"{cfg.worker.base_url}/health", headers=headers)
            health.raise_for_status()
            jobs = await client.get(f"{cfg.worker.base_url}/jobs", headers=headers)
            jobs.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        return {"reachable": False, "error": _short_error(exc)}

    job_rows = jobs.json().get("jobs", [])
    return {
        "reachable": True,
        "health": health.json(),
        "jobs": {
            "total": len(job_rows),
            "running": sum(1 for j in job_rows if j.get("status") == "running"),
            "recent": [
                {
                    "name": j.get("name") or j.get("label") or j.get("id"),
                    "status": j.get("status"),
                    "branch": j.get("branch") or "",
                }
                for j in job_rows[-5:]
            ],
        },
    }


def launchd_status(label: str) -> dict[str, Any]:
    if platform.system() != "Darwin":
        return {"label": label, "available": False, "reason": "launchd is macOS-only"}
    target = f"gui/{os.getuid()}/{label}"
    try:
        result = _run(["launchctl", "print", target], timeout=2.0)
    except Exception as exc:  # noqa: BLE001
        return {"label": label, "available": False, "error": _short_error(exc)}
    if result.returncode != 0:
        return {"label": label, "available": True, "loaded": False}
    state = ""
    pid = ""
    for line in result.stdout.splitlines():
        s = line.strip()
        if s.startswith("state ="):
            state = s.partition("=")[2].strip()
        elif s.startswith("pid ="):
            pid = s.partition("=")[2].strip()
    return {
        "label": label,
        "available": True,
        "loaded": True,
        "state": state or "unknown",
        "pid": int(pid) if pid.isdigit() else None,
    }


def git_status(cwd: str) -> dict[str, Any]:
    try:
        root = _run(["git", "rev-parse", "--show-toplevel"], cwd=cwd)
        if root.returncode != 0:
            return {"available": False}
        branch = _run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=cwd)
        commit = _run(["git", "rev-parse", "--short", "HEAD"], cwd=cwd)
        dirty = _run(["git", "status", "--porcelain"], cwd=cwd)
    except Exception as exc:  # noqa: BLE001
        return {"available": False, "error": _short_error(exc)}
    return {
        "available": True,
        "root": root.stdout.strip(),
        "branch": branch.stdout.strip() if branch.returncode == 0 else "",
        "commit": commit.stdout.strip() if commit.returncode == 0 else "",
        "dirty": bool(dirty.stdout.strip()) if dirty.returncode == 0 else False,
    }


def docker_compose_status(cwd: str) -> dict[str, Any]:
    if not _has_compose_file(cwd):
        return {
            "available": False,
            "configured": False,
            "status": "not_configured",
            "detail": "No local Docker compose project found.",
            "services": [],
        }
    try:
        result = _run(["docker", "compose", "ps", "--format", "json"], cwd=cwd, timeout=4.0)
    except FileNotFoundError:
        return {"available": False, "error": "docker not found"}
    except Exception as exc:  # noqa: BLE001
        return {"available": False, "error": _short_error(exc)}
    if result.returncode != 0:
        return {"available": False, "error": result.stderr.strip() or result.stdout.strip()}

    services: list[dict[str, Any]] = []
    text = result.stdout.strip()
    if text:
        try:
            parsed = json.loads(text)
            services = parsed if isinstance(parsed, list) else [parsed]
        except json.JSONDecodeError:
            for line in text.splitlines():
                try:
                    services.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return {
        "available": True,
        "configured": True,
        "services": [
            {
                "name": s.get("Service") or s.get("Name") or s.get("name") or "",
                "state": s.get("State") or s.get("state") or "",
                "status": s.get("Status") or s.get("status") or "",
            }
            for s in services
        ],
    }


def _has_compose_file(cwd: str) -> bool:
    root = Path(cwd)
    return any(
        (root / name).is_file()
        for name in (
            "compose.yaml",
            "compose.yml",
            "docker-compose.yaml",
            "docker-compose.yml",
        )
    )


async def collect_fleet_status(
    cfg: Config, *, cwd: str | None = None, include_docker: bool = True
) -> dict[str, Any]:
    cwd = cwd or os.getcwd()
    brain_probe, worker_probe = await asyncio.gather(probe_brain(cfg), probe_worker(cfg))
    return {
        "version": __version__,
        "device_id": cfg.capabilities.device_id,
        "platform": {
            "system": platform.system(),
            "machine": platform.machine(),
            "python": platform.python_version(),
        },
        "services": {role: launchd_status(label) for role, label in SERVICE_LABELS.items()},
        "brain": {
            "bind": f"{cfg.brain.host}:{cfg.brain.port}",
            "auth_configured": bool(
                cfg.brain.pairing_token.get_secret_value() or cfg.brain.devices
            ),
            "devices": [
                {"device_id": d.device_id, "identity": d.identity or "house"}
                for d in cfg.brain.devices
            ],
        },
        "intercom": {
            "brain_url": cfg.intercom.brain_url,
            "device_id": cfg.capabilities.device_id,
            "pairing": brain_probe,
        },
        "worker": {
            "base_url": cfg.worker.base_url,
            "agent": cfg.worker.agent,
            "workspace": cfg.worker.workspace,
            "probe": worker_probe,
        },
        "docker": docker_compose_status(cwd) if include_docker else {"available": False},
        "git": git_status(cwd),
    }
