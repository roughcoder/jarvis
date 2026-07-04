from __future__ import annotations

import json
import os
import pathlib
import socket
from collections.abc import Callable
from typing import Any

import httpx

from jarvis.config import WorkerConfig
from jarvis.engines import engine_ids, normalize_engine_id, worker_supports_engine
from jarvis.ids import utc_now
from jarvis.worker_session_contract import ACTIVE_SESSION_STATUSES, SESSION_RUNNING
from jarvis.orchestration.models import WorkerProfile

_PROFILE_CACHE_TTL_S = 2.0
_PROFILE_CACHE: dict[str, tuple[int, float, list[dict[str, Any]]]] = {}


class WorkerRegistry:
    def __init__(
        self,
        worker_cfg: WorkerConfig,
        *,
        profiles_path: str = "",
        http_get: Callable[..., Any] | None = None,
    ) -> None:
        self.worker_cfg = worker_cfg
        self.profiles_path = pathlib.Path(profiles_path).expanduser() if profiles_path else None
        self._http_get = http_get or httpx.get
        self._dotenv_cache: dict[str, str] | None = None

    def profiles(self, *, probe: bool = False) -> list[WorkerProfile]:
        profiles = self._load_profiles()
        if not profiles:
            profiles = [self._default_profile()]
        if probe:
            profiles = [self._probe(p) for p in profiles]
        return profiles

    def get(self, worker_id: str = "", *, probe: bool = False) -> WorkerProfile | None:
        profiles = self.profiles(probe=probe)
        if not worker_id:
            return profiles[0] if profiles else None
        return next((p for p in profiles if p.worker_id == worker_id), None)

    def choose(
        self,
        required: list[str] | None = None,
        preferred: list[str] | None = None,
        *,
        engine: str = "",
        engines: list[str] | None = None,
        slots: int = 1,
    ) -> WorkerProfile | None:
        required_set = set(required or [])
        required_engines = _required_engines(engines or [])
        if engine and not required_engines:
            required_engines = [engine]
        profiles = self.profiles(probe=True)
        if preferred:
            by_id = {p.worker_id: p for p in profiles}
            ordered = [by_id[p] for p in preferred if p in by_id] + [p for p in profiles if p.worker_id not in preferred]
        else:
            ordered = profiles
        for profile in ordered:
            if profile.status == "offline":
                continue
            if any(not worker_supports_engine(profile.supported_engines, target) for target in required_engines):
                continue
            if required_set.issubset(set(profile.capabilities)):
                if profile.current_jobs + max(1, slots) <= profile.max_concurrent_jobs:
                    return profile
        return None

    def _default_profile(self) -> WorkerProfile:
        return WorkerProfile(
            worker_id="local-worker",
            display_name=local_worker_display_name(),
            capabilities=["git", "python", "uv", "codex", "shell"],
            base_url=self.worker_cfg.base_url,
            token_set=bool(self.worker_cfg.token.get_secret_value()),
            max_concurrent_jobs=1,
            agent=self.worker_cfg.agent,
            default_engine=self.worker_cfg.agent,
            supported_engines=engine_ids(self.worker_cfg.supported_engines, default_engine=self.worker_cfg.agent),
        )

    def _load_profiles(self) -> list[WorkerProfile]:
        if self.profiles_path is None or not self.profiles_path.exists():
            return []
        try:
            stat = self.profiles_path.stat()
            cache_key = str(self.profiles_path)
            cached = _PROFILE_CACHE.get(cache_key)
            now = time_monotonic()
            if cached is not None and cached[0] == stat.st_mtime_ns and cached[1] > now:
                items = cached[2]
            else:
                raw = json.loads(self.profiles_path.read_text())
                if isinstance(raw, list):
                    items = raw
                elif isinstance(raw, dict):
                    items = raw.get("workers", [])
                else:
                    items = []
                if not isinstance(items, list):
                    items = []
                items = [dict(item) for item in items if isinstance(item, dict)]
                _PROFILE_CACHE[cache_key] = (stat.st_mtime_ns, now + _PROFILE_CACHE_TTL_S, items)
        except (OSError, json.JSONDecodeError):
            return []
        profiles: list[WorkerProfile] = []
        for item in items:
            try:
                profile = WorkerProfile.from_dict(item)
                if profile.token_env and self._token_env_value(profile.token_env):
                    profile.token_set = True
                profiles.append(profile)
            except (TypeError, KeyError):
                continue
        return profiles

    def _probe(self, profile: WorkerProfile) -> WorkerProfile:
        if not profile.base_url:
            profile.status = "unknown"
            return profile
        headers = {}
        token = self._token_env_value(profile.token_env) if profile.token_env else ""
        if not token and profile.worker_id == "local-worker":
            token = self.worker_cfg.token.get_secret_value()
        profile.token_set = bool(token)
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            health = self._http_get(f"{profile.base_url}/health", headers=headers, timeout=3)
            health.raise_for_status()
            jobs = self._http_get(f"{profile.base_url}/jobs", headers=headers, timeout=3)
            jobs.raise_for_status()
            job_data = jobs.json().get("jobs", [])
            session_data = self._session_data(profile.base_url, headers)
        except Exception:  # noqa: BLE001 - status probe must not crash CLI
            profile.status = "offline"
            profile.current_jobs = 0
            return profile
        profile.status = "online"
        profile.last_seen_at = utc_now()
        profile.current_jobs = sum(1 for j in job_data if j.get("status") == SESSION_RUNNING) + sum(
            1 for s in session_data if s.get("status") in ACTIVE_SESSION_STATUSES
        )
        data = health.json()
        if data.get("agent"):
            profile.agent = data["agent"]
        if data.get("default_engine") or data.get("agent"):
            profile.default_engine = data.get("default_engine") or data.get("agent")
        if data.get("supported_engines"):
            profile.supported_engines = engine_ids(
                data["supported_engines"],
                default_engine=profile.default_engine or profile.agent,
            )
        if isinstance(data.get("engine_supports"), dict):
            profile.engine_supports = _engine_supports_from_mapping(data["engine_supports"])
        elif isinstance(data.get("engines"), list):
            profile.engine_supports = _engine_supports_from_rows(data["engines"])
        if isinstance(data.get("repositories"), list):
            profile.repositories = [dict(item) for item in data["repositories"] if isinstance(item, dict)]
        profile.system = _system_from_health(data.get("system"))
        profile.__post_init__()
        return profile

    def _session_data(self, base_url: str, headers: dict[str, str]) -> list[dict[str, Any]]:
        try:
            sessions = self._http_get(f"{base_url}/sessions", headers=headers, timeout=3)
            sessions.raise_for_status()
            data = sessions.json().get("sessions", [])
        except Exception:  # noqa: BLE001 - older workers may not expose sessions yet
            return []
        return [dict(item) for item in data if isinstance(item, dict)]

    def _token_env_value(self, name: str) -> str:
        if not name:
            return ""
        value = os.environ.get(name, "")
        if value:
            return value
        return self._dotenv_values().get(name, "")

    def _dotenv_values(self) -> dict[str, str]:
        if self._dotenv_cache is not None:
            return self._dotenv_cache
        path = pathlib.Path(os.environ.get("JARVIS_ENV_FILE") or ".env").expanduser()
        values: dict[str, str] = {}
        try:
            lines = path.read_text().splitlines()
        except OSError:
            self._dotenv_cache = values
            return values
        for raw in lines:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key:
                values[key] = value
        self._dotenv_cache = values
        return values


def local_worker_display_name() -> str:
    hostname = socket.gethostname().split(".", 1)[0].strip()
    return f"{hostname} worker" if hostname else "Local worker"


def _required_engines(engines: list[str]) -> list[str]:
    result: list[str] = []
    for value in engines:
        engine = normalize_engine_id(value)
        if engine and engine not in result:
            result.append(engine)
    return result


def time_monotonic() -> float:
    import time

    return time.monotonic()


def _engine_supports_from_mapping(raw: dict[str, Any]) -> dict[str, dict[str, bool]]:
    result: dict[str, dict[str, bool]] = {}
    for engine, supports in raw.items():
        if isinstance(supports, dict):
            result[str(engine)] = {str(key): bool(value) for key, value in supports.items()}
    return result


def _engine_supports_from_rows(rows: list[Any]) -> dict[str, dict[str, bool]]:
    result: dict[str, dict[str, bool]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        engine = str(row.get("engine") or "")
        supports = row.get("supports")
        if engine and isinstance(supports, dict):
            result[engine] = {str(key): bool(value) for key, value in supports.items()}
    return result


def _system_from_health(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    result: dict[str, Any] = {
        "hostname": _string_or_none(raw.get("hostname")),
        "platform": _string_or_none(raw.get("platform")),
        "arch": _string_or_none(raw.get("arch")),
        "os_name": _string_or_none(raw.get("os_name")),
        "os_version": _string_or_none(raw.get("os_version")),
        "kernel_version": _string_or_none(raw.get("kernel_version")),
        "cpu_model": _string_or_none(raw.get("cpu_model")),
        "cpu_cores_physical": _int_or_none(raw.get("cpu_cores_physical")),
        "cpu_cores_logical": _int_or_none(raw.get("cpu_cores_logical")),
        "memory_total_bytes": _int_or_none(raw.get("memory_total_bytes")),
        "memory_available_bytes": _int_or_none(raw.get("memory_available_bytes")),
        "memory_used_bytes": _int_or_none(raw.get("memory_used_bytes")),
        "memory_used_percent": _float_or_none(raw.get("memory_used_percent")),
        "load_average": _load_average(raw.get("load_average")),
        "uptime_seconds": _int_or_none(raw.get("uptime_seconds")),
        "disk": _system_disks(raw.get("disk")),
        "gpu": _system_gpu(raw.get("gpu")),
        "checked_at": _string_or_none(raw.get("checked_at")),
    }
    return result


def _system_disks(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    disks: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        disks.append(
            {
                "mount": _safe_mount(item.get("mount")),
                "filesystem": _string_or_none(item.get("filesystem")),
                "total_bytes": _int_or_none(item.get("total_bytes")),
                "available_bytes": _int_or_none(item.get("available_bytes")),
                "used_bytes": _int_or_none(item.get("used_bytes")),
                "used_percent": _float_or_none(item.get("used_percent")),
            }
        )
    return disks


def _system_gpu(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    result: list[dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict):
            result.append(
                {
                    "name": _string_or_none(item.get("name")),
                    "memory_total_bytes": _int_or_none(item.get("memory_total_bytes")),
                }
            )
    return result


def _load_average(raw: Any) -> list[float | None]:
    if not isinstance(raw, list):
        return [None, None, None]
    values = [_float_or_none(value) for value in raw[:3]]
    return [*values, *([None] * (3 - len(values)))]


def _safe_mount(raw: Any) -> str | None:
    text = _string_or_none(raw)
    if not text:
        return None
    if text == "/":
        return "/"
    return None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None
