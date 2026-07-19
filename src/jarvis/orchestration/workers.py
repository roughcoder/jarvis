from __future__ import annotations

import hmac
import json
import os
import pathlib
import socket
from collections.abc import Callable
from typing import Any

import httpx
from dotenv import dotenv_values

from jarvis.config import WorkerConfig
from jarvis.engines import engine_ids, normalize_engine_id, worker_supports_engine
from jarvis.ids import utc_now
from jarvis.worker_session_contract import ACTIVE_SESSION_STATUSES, SESSION_RUNNING
from jarvis.orchestration.models import WorkerProfile

_PROFILE_CACHE_TTL_S = 2.0
_PROFILE_CACHE: dict[str, tuple[int, float, list[dict[str, Any]]]] = {}
# Fields that only ever come from a live /health probe. `workers.json` on disk
# carries none of them, so an unprobed read used to report a worker as never
# seen, of unknown version, with zero worktrees — indistinguishable from a
# healthy empty worker. Remember the last successful probe per worker so
# unprobed reads (the cockpit's default) report last-known truth instead.
_PROBE_SNAPSHOT_FIELDS = ("last_seen_at", "runtime", "system", "worktree_inventory")
# Bounded: a worker that has been unreachable for this long should read as
# unknown again rather than keep showing numbers from a machine that may since
# have been rebuilt.
_PROBE_SNAPSHOT_TTL_S = 30 * 60.0
_PROBE_SNAPSHOTS: dict[str, tuple[float, dict[str, Any]]] = {}


def reset_probe_snapshots() -> None:
    """Drop remembered probe results (process-global; used by tests)."""
    _PROBE_SNAPSHOTS.clear()


def _probe_snapshot_key(profile: WorkerProfile) -> str:
    return f"{profile.worker_id}@{profile.base_url}"


def _remember_probe(profile: WorkerProfile) -> None:
    snapshot = {field: getattr(profile, field) for field in _PROBE_SNAPSHOT_FIELDS}
    _PROBE_SNAPSHOTS[_probe_snapshot_key(profile)] = (time_monotonic() + _PROBE_SNAPSHOT_TTL_S, snapshot)


def _apply_probe_snapshot(profile: WorkerProfile) -> WorkerProfile:
    entry = _PROBE_SNAPSHOTS.get(_probe_snapshot_key(profile))
    if entry is None:
        return profile
    expires_at, snapshot = entry
    if expires_at <= time_monotonic():
        _PROBE_SNAPSHOTS.pop(_probe_snapshot_key(profile), None)
        return profile
    for field, value in snapshot.items():
        if not getattr(profile, field, None):
            setattr(profile, field, value)
    return profile
# The orchestration API makes many small worker reads while assembling cockpit
# projections.  Keep one process-local client so those reads reuse connections
# instead of paying a TCP/TLS setup cost for every row.  Callers can still
# inject a request callable for tests or a request-scoped transport.
_WORKER_HTTP_CLIENT = httpx.Client()


def worker_http_get(*args: Any, **kwargs: Any) -> httpx.Response:
    return _WORKER_HTTP_CLIENT.get(*args, **kwargs)


def worker_http_post(*args: Any, **kwargs: Any) -> httpx.Response:
    return _WORKER_HTTP_CLIENT.post(*args, **kwargs)


class WorkerRegistry:
    def __init__(
        self,
        worker_cfg: WorkerConfig,
        *,
        profiles_path: str = "",
        http_get: Callable[..., Any] | None = None,
        http_post: Callable[..., Any] | None = None,
    ) -> None:
        self.worker_cfg = worker_cfg
        self.profiles_path = pathlib.Path(profiles_path).expanduser() if profiles_path else None
        self._http_get = http_get or worker_http_get
        self._http_post = http_post or worker_http_post

    def profiles(self, *, probe: bool = False) -> list[WorkerProfile]:
        profiles = self._load_profiles()
        if not profiles:
            profiles = [self._default_profile()]
        if probe:
            return [self._probe(p) for p in profiles]
        return [_apply_probe_snapshot(p) for p in profiles]

    def get(self, worker_id: str = "", *, probe: bool = False) -> WorkerProfile | None:
        profiles = self.profiles(probe=probe)
        if not worker_id:
            return profiles[0] if profiles else None
        return next((p for p in profiles if p.worker_id == worker_id), None)

    def authenticate_token(self, token: str) -> WorkerProfile | None:
        """Return the configured worker that owns a presented notify token."""
        if not token:
            return None
        for profile in self.profiles(probe=False):
            expected = self._headers(profile).get("Authorization", "").removeprefix("Bearer ")
            if expected and hmac.compare_digest(token, expected):
                return profile
        return None

    def with_repo_access(self, profiles: list[WorkerProfile], repo: str) -> list[WorkerProfile]:
        if not repo or not _should_probe_repo_access(repo):
            return profiles
        return [self._probe_repo_access(profile, repo) for profile in profiles]

    def configured_profile_count(self) -> int:
        """Profiles available for dispatch, including the default fallback.

        An explicit but empty workers file means no named workers are configured.
        A missing file preserves the existing local-worker fallback used by
        `profiles()` and dispatch selection.
        """
        if self.profiles_path is None or not self.profiles_path.exists():
            return 1
        return len(self._load_profiles())

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
            max_concurrent_jobs=max(1, self.worker_cfg.max_concurrent_jobs),
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
        headers = self._headers(profile)
        token = headers.get("Authorization", "").removeprefix("Bearer ")
        profile.token_set = bool(token)
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
        if isinstance(data.get("git_identity"), dict):
            profile.git_identity = dict(data["git_identity"])
        if isinstance(data.get("repo_access"), list):
            profile.repo_access = [dict(item) for item in data["repo_access"] if isinstance(item, dict)]
        if isinstance(data.get("repositories"), list):
            profile.repositories = [dict(item) for item in data["repositories"] if isinstance(item, dict)]
        if isinstance(data.get("diagnostics"), dict):
            diagnostics = dict(data["diagnostics"])
            profile.readiness = diagnostics
            if isinstance(diagnostics.get("git_identity"), dict):
                profile.git_identity = dict(diagnostics["git_identity"])
            if isinstance(diagnostics.get("repositories"), list):
                profile.repositories = [
                    dict(item) for item in diagnostics["repositories"] if isinstance(item, dict)
                ]
        if isinstance(data.get("worktree_inventory"), dict):
            profile.worktree_inventory = dict(data["worktree_inventory"])
        if isinstance(data.get("runtime"), dict):
            profile.runtime = {str(key): _string_or_none(value) for key, value in data["runtime"].items()}
        profile.system = _system_from_health(data.get("system"))
        profile.__post_init__()
        _remember_probe(profile)
        return profile

    def _probe_repo_access(self, profile: WorkerProfile, repo: str) -> WorkerProfile:
        if _repo_access_row(profile, repo) is not None:
            return profile
        if profile.status == "offline" or not profile.base_url:
            return profile
        headers = self._headers(profile)
        try:
            response = self._http_post(
                f"{profile.base_url}/run",
                json={"action": "repo_access", "args": {"repo": repo}},
                headers=headers,
                timeout=self.worker_cfg.request_timeout_s,
            )
            response.raise_for_status()
            data = response.json()
        except Exception as exc:  # noqa: BLE001 - validation reports unknown rather than crashing
            profile.repo_access = [
                *profile.repo_access,
                {
                    "repo": repo,
                    "accessible": False,
                    "public": False,
                    "reason_code": "repo-access-probe-failed",
                    "reason": str(exc)[:200] or exc.__class__.__name__,
                },
            ]
            profile.__post_init__()
            return profile
        access = data.get("access") if isinstance(data, dict) else None
        if isinstance(access, dict):
            profile.repo_access = [*profile.repo_access, dict(access)]
            identity = access.get("git_identity")
            if isinstance(identity, dict) and not profile.git_identity:
                profile.git_identity = dict(identity)
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
        return worker_token_value(name)

    def _headers(self, profile: WorkerProfile) -> dict[str, str]:
        return worker_auth_headers(self.worker_cfg, profile)


def local_worker_display_name() -> str:
    hostname = socket.gethostname().split(".", 1)[0].strip()
    return f"{hostname} worker" if hostname else "Local worker"


def worker_token_value(name: str) -> str:
    if not name:
        return ""
    value = os.environ.get(name, "")
    if value:
        return value
    parsed = dotenv_values(_jarvis_env_path())
    raw = parsed.get(name)
    return raw or ""


def _jarvis_env_path() -> pathlib.Path:
    return pathlib.Path(os.environ.get("JARVIS_ENV_FILE") or ".env").expanduser()


def worker_auth_headers(worker_cfg: WorkerConfig, worker: WorkerProfile | None) -> dict[str, str]:
    """Bearer auth header for a worker, falling back to the local-worker token."""
    token = worker_token_value(worker.token_env) if worker and worker.token_env else ""
    if not token and (worker is None or worker.worker_id == "local-worker"):
        token = worker_cfg.token.get_secret_value()
    return {"Authorization": f"Bearer {token}"} if token else {}


def resolve_worker_endpoint(worker_cfg: WorkerConfig, worker: WorkerProfile | None) -> tuple[str, dict[str, str]]:
    """Base URL + auth headers for dispatching to `worker`, or the local worker."""
    if worker is None:
        base_url = worker_cfg.base_url
    elif worker.base_url:
        base_url = worker.base_url
    elif worker.worker_id == "local-worker":
        base_url = worker_cfg.base_url
    else:
        raise RuntimeError(f"worker {worker.worker_id} has no base_url; refusing to route to local worker")
    return base_url, worker_auth_headers(worker_cfg, worker)


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


def _repo_access_row(profile: WorkerProfile, repo: str) -> dict[str, Any] | None:
    for row in profile.repo_access:
        candidate = str(row.get("repo") or "")
        if repo_ref_matches_access_row(repo, candidate):
            return row
    return None


def _should_probe_repo_access(repo_ref: str) -> bool:
    text = str(repo_ref or "").strip()
    if not text:
        return False
    if pathlib.Path(text).expanduser().is_absolute():
        return False
    if text.startswith(("http://", "https://", "git@")):
        return "github.com" in text
    return text.count("/") == 1


def repo_ref_matches_access_row(requested: str, candidate: str) -> bool:
    if not candidate:
        return False
    if _is_owner_name_ref(requested):
        return candidate == requested
    repo_name = requested.rsplit("/", 1)[-1]
    return candidate in {requested, repo_name} or candidate.rsplit("/", 1)[-1] == repo_name


def _is_owner_name_ref(value: str) -> bool:
    text = str(value or "").strip()
    return text.count("/") == 1 and not text.startswith(("http://", "https://", "git@"))


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
