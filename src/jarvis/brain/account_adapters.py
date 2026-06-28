"""Provider-neutral account adapter contracts.

The brain and tools talk in email/calendar domain operations. Provider details
such as `gogcli`, Google, or Microsoft Graph stay behind these adapters and are
selected by account binding metadata.
"""

from __future__ import annotations

import asyncio
import shutil
from dataclasses import dataclass, field
from typing import Any, Protocol

from jarvis.brain.accounts import AccountBinding
from jarvis.config import GoogleConfig


class EmailAdapter(Protocol):
    async def search(self, binding: AccountBinding, query: str, *, max_results: int | None = None) -> str: ...
    async def get_message(self, binding: AccountBinding, message_id: str, *, body_mode: str = "summary") -> str: ...
    async def create_draft(self, binding: AccountBinding, message: dict[str, Any]) -> str: ...
    async def send(self, binding: AccountBinding, message: dict[str, Any]) -> str: ...
    async def modify(self, binding: AccountBinding, message_ids: list[str], changes: dict[str, Any]) -> str: ...


class CalendarAdapter(Protocol):
    async def freebusy(self, binding: AccountBinding, start: str, end: str) -> str: ...
    async def list_events(self, binding: AccountBinding, *, days: int) -> str: ...
    async def create_event(self, binding: AccountBinding, event: dict[str, Any], *, send_updates: bool) -> str: ...
    async def update_event(
        self,
        binding: AccountBinding,
        event_id: str,
        patch: dict[str, Any],
        *,
        send_updates: bool,
    ) -> str: ...
    async def delete_event(self, binding: AccountBinding, event_id: str, *, send_updates: bool) -> str: ...
    async def respond_to_invite(self, binding: AccountBinding, event_id: str, response: str) -> str: ...


@dataclass(frozen=True)
class AdapterCall:
    operation: str
    binding: AccountBinding
    payload: dict[str, Any] = field(default_factory=dict)


class FakeAccountAdapter:
    """Hermetic email/calendar adapter for router and tool tests."""

    def __init__(self, responses: dict[str, str] | None = None) -> None:
        self.responses = responses or {}
        self.calls: list[AdapterCall] = []

    def _record(self, operation: str, binding: AccountBinding, **payload: Any) -> str:
        self.calls.append(AdapterCall(operation, binding, payload))
        return self.responses.get(operation, f"{operation}: ok")

    async def search(self, binding: AccountBinding, query: str, *, max_results: int | None = None) -> str:
        return self._record("email.search", binding, query=query, max_results=max_results)

    async def get_message(self, binding: AccountBinding, message_id: str, *, body_mode: str = "summary") -> str:
        return self._record("email.get_message", binding, message_id=message_id, body_mode=body_mode)

    async def create_draft(self, binding: AccountBinding, message: dict[str, Any]) -> str:
        return self._record("email.create_draft", binding, message=message)

    async def send(self, binding: AccountBinding, message: dict[str, Any]) -> str:
        return self._record("email.send", binding, message=message)

    async def modify(self, binding: AccountBinding, message_ids: list[str], changes: dict[str, Any]) -> str:
        return self._record("email.modify", binding, message_ids=message_ids, changes=changes)

    async def freebusy(self, binding: AccountBinding, start: str, end: str) -> str:
        return self._record("calendar.freebusy", binding, start=start, end=end)

    async def list_events(self, binding: AccountBinding, *, days: int) -> str:
        return self._record("calendar.list_events", binding, days=days)

    async def create_event(self, binding: AccountBinding, event: dict[str, Any], *, send_updates: bool) -> str:
        return self._record("calendar.create_event", binding, event=event, send_updates=send_updates)

    async def update_event(
        self,
        binding: AccountBinding,
        event_id: str,
        patch: dict[str, Any],
        *,
        send_updates: bool,
    ) -> str:
        return self._record(
            "calendar.update_event",
            binding,
            event_id=event_id,
            patch=patch,
            send_updates=send_updates,
        )

    async def delete_event(self, binding: AccountBinding, event_id: str, *, send_updates: bool) -> str:
        return self._record("calendar.delete_event", binding, event_id=event_id, send_updates=send_updates)

    async def respond_to_invite(self, binding: AccountBinding, event_id: str, response: str) -> str:
        return self._record("calendar.respond_to_invite", binding, event_id=event_id, response=response)


class GogcliAccountAdapter:
    """Google/Gmail/Workspace adapter backed by OpenClaw `gogcli`."""

    def __init__(self, cfg: GoogleConfig) -> None:
        self.cfg = cfg

    async def _run(self, args: list[str], *, enabled: str) -> str:
        if not shutil.which(self.cfg.gogcli_bin):
            return "google isn't set up yet — run `jarvis google-setup` first."
        try:
            proc = await asyncio.create_subprocess_exec(
                self.cfg.gogcli_bin,
                "--plain",
                "--no-input",
                f"--enable-commands-exact={enabled}",
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            out, _ = await asyncio.wait_for(proc.communicate(), self.cfg.timeout_s)
        except TimeoutError:
            return "error: google timed out"
        except Exception as exc:  # noqa: BLE001
            return f"error: couldn't run google ({exc})"
        text = (out or b"").decode("utf-8", "replace").strip()
        return text or "(no output)"

    async def search(self, binding: AccountBinding, query: str, *, max_results: int | None = None) -> str:
        args = ["gmail", "search", "--query", query]
        if max_results is not None:
            args.extend(["--max-results", str(max_results)])
        return await self._run(args, enabled="gmail.search")

    async def get_message(self, binding: AccountBinding, message_id: str, *, body_mode: str = "summary") -> str:
        return "error: gogcli message fetch is not wired yet"

    async def create_draft(self, binding: AccountBinding, message: dict[str, Any]) -> str:
        return "error: gogcli draft creation is not wired yet"

    async def send(self, binding: AccountBinding, message: dict[str, Any]) -> str:
        return await self._run(
            [
                "gmail",
                "send",
                "--to",
                str(message.get("to") or ""),
                "--subject",
                str(message.get("subject") or ""),
                "--body",
                str(message.get("body") or ""),
            ],
            enabled="gmail.send",
        )

    async def modify(self, binding: AccountBinding, message_ids: list[str], changes: dict[str, Any]) -> str:
        return "error: gogcli mail cleanup is not wired yet"

    async def freebusy(self, binding: AccountBinding, start: str, end: str) -> str:
        return "error: gogcli free/busy is not wired yet"

    async def list_events(self, binding: AccountBinding, *, days: int) -> str:
        return await self._run(["calendar", "events", "--days", str(days)], enabled="calendar.events")

    async def create_event(self, binding: AccountBinding, event: dict[str, Any], *, send_updates: bool) -> str:
        return "error: gogcli calendar writes are not wired yet"

    async def update_event(
        self,
        binding: AccountBinding,
        event_id: str,
        patch: dict[str, Any],
        *,
        send_updates: bool,
    ) -> str:
        return "error: gogcli calendar writes are not wired yet"

    async def delete_event(self, binding: AccountBinding, event_id: str, *, send_updates: bool) -> str:
        return "error: gogcli calendar writes are not wired yet"

    async def respond_to_invite(self, binding: AccountBinding, event_id: str, response: str) -> str:
        return "error: gogcli RSVP is not wired yet"
