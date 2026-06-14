"""Claude Managed Agents client (Phase 3 cloud lane).

A thin async wrapper over the Managed Agents REST API (api.anthropic.com). Used
by `jarvis remote-setup` to create the agent + environment once, and by the
remote coding tools to start a cloud session and send it a task. Imports nothing
from the brain.
"""

from __future__ import annotations

from typing import Any

import httpx

from jarvis.config import RemoteConfig


class RemoteClient:
    def __init__(self, cfg: RemoteConfig) -> None:
        self._cfg = cfg

    def _headers(self) -> dict[str, str]:
        return {
            "x-api-key": self._cfg.api_key.get_secret_value(),
            "anthropic-version": "2023-06-01",
            "anthropic-beta": self._cfg.beta,
            "content-type": "application/json",
        }

    async def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self._cfg.request_timeout_s) as client:
            r = await client.post(f"{self._cfg.base_url}{path}", json=body, headers=self._headers())
            r.raise_for_status()
            return r.json()

    async def _get(self, path: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self._cfg.request_timeout_s) as client:
            r = await client.get(f"{self._cfg.base_url}{path}", headers=self._headers())
            r.raise_for_status()
            return r.json()

    # --- one-time setup ----------------------------------------------------
    async def create_agent(self, name: str, system: str) -> dict[str, Any]:
        return await self._post(
            "/v1/agents",
            {
                "name": name,
                "model": self._cfg.model,
                "system": system,
                "tools": [{"type": "agent_toolset_20260401"}],  # bash, files, web, ...
            },
        )

    async def create_environment(self, name: str) -> dict[str, Any]:
        return await self._post(
            "/v1/environments",
            {"name": name, "config": {"type": "cloud", "networking": {"type": "unrestricted"}}},
        )

    # --- per-job -----------------------------------------------------------
    async def create_session(self, title: str) -> dict[str, Any]:
        return await self._post(
            "/v1/sessions",
            {
                "agent": self._cfg.agent_id,
                "environment_id": self._cfg.environment_id,
                "title": title,
            },
        )

    async def send_task(self, session_id: str, text: str) -> dict[str, Any]:
        return await self._post(
            f"/v1/sessions/{session_id}/events",
            {"events": [{"type": "user.message", "content": [{"type": "text", "text": text}]}]},
        )

    async def get_session(self, session_id: str) -> dict[str, Any]:
        return await self._get(f"/v1/sessions/{session_id}")
