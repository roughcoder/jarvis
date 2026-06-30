from __future__ import annotations

import json
import os
import pathlib
from collections.abc import Callable
from typing import Any

import httpx

from jarvis.config import WorkerConfig
from jarvis.engines import engine_ids, worker_supports_engine
from jarvis.orchestration.models import WorkerProfile


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
    ) -> WorkerProfile | None:
        required_set = set(required or [])
        profiles = self.profiles(probe=True)
        if preferred:
            by_id = {p.worker_id: p for p in profiles}
            ordered = [by_id[p] for p in preferred if p in by_id] + [p for p in profiles if p.worker_id not in preferred]
        else:
            ordered = profiles
        for profile in ordered:
            if profile.status == "offline":
                continue
            if engine and not worker_supports_engine(profile.supported_engines, engine):
                continue
            if required_set.issubset(set(profile.capabilities)):
                if profile.current_jobs < profile.max_concurrent_jobs:
                    return profile
        return None

    def _default_profile(self) -> WorkerProfile:
        return WorkerProfile(
            worker_id="local-worker",
            display_name="Local worker",
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
            raw = json.loads(self.profiles_path.read_text())
        except (OSError, json.JSONDecodeError):
            return []
        items = raw if isinstance(raw, list) else raw.get("workers", [])
        profiles: list[WorkerProfile] = []
        for item in items:
            try:
                profile = WorkerProfile.from_dict(item)
                if profile.token_env and os.environ.get(profile.token_env):
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
        token = os.environ.get(profile.token_env, "") if profile.token_env else ""
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
        except Exception:  # noqa: BLE001 - status probe must not crash CLI
            profile.status = "offline"
            profile.current_jobs = 0
            return profile
        profile.status = "online"
        profile.current_jobs = sum(1 for j in job_data if j.get("status") == "running")
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
        profile.__post_init__()
        return profile
