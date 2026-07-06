from __future__ import annotations

import contextlib
import fcntl
import json
import pathlib
import re
from collections import deque
from typing import Any

from jarvis.ids import new_id, utc_now
from jarvis.orchestration.redaction import redact


_PROJECT_ID = re.compile(r"^[A-Za-z0-9_-]+$")
MAX_ACTIVITY_SCAN_LINES = 5000


class ProjectActivityLog:
    """Append-only per-project activity log for cockpit project writes."""

    def __init__(self, root: str) -> None:
        self.root = pathlib.Path(root).expanduser() / "project-activity"
        self.root.mkdir(parents=True, exist_ok=True)

    def append(
        self,
        project_id: str,
        activity_type: str,
        actor: dict[str, Any],
        summary: str,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        event = {
            "id": new_id("act"),
            "project_id": project_id,
            "type": activity_type,
            "actor": _redacted(actor),
            "summary": redact(summary),
            "data": _redacted(data or {}),
            "occurred_at": utc_now(),
        }
        with self._locked(project_id):
            with self._path(project_id).open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(event, sort_keys=True) + "\n")
                handle.flush()
        return event

    def list(
        self,
        project_id: str,
        *,
        limit: int,
        cursor: str = "",
        activity_type: str = "",
    ) -> tuple[list[dict[str, Any]], str]:
        events = [
            event
            for event in reversed(self._read(project_id))
            if not activity_type or str(event.get("type") or "") == activity_type
        ]
        if cursor:
            for idx, event in enumerate(events):
                if str(event.get("id") or "") == cursor:
                    events = events[idx + 1 :]
                    break
            else:
                from jarvis.orchestration.cockpit import CockpitError

                raise CockpitError(
                    "stale_cursor",
                    "unknown pagination cursor; clear the cursor and refetch from the first page",
                    recoverable=True,
                    status=400,
                )
        page = events[:limit]
        next_cursor = str(page[-1].get("id") or "") if len(events) > limit and page else ""
        return page, next_cursor

    def _read(self, project_id: str) -> list[dict[str, Any]]:
        path = self._path(project_id)
        if not path.exists():
            return []
        lines: deque[str] = deque(maxlen=MAX_ACTIVITY_SCAN_LINES)
        with path.open("r", encoding="utf-8") as handle:
            lines.extend(handle)
        rows: list[dict[str, Any]] = []
        for line in lines:
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                rows.append(item)
        return rows

    def _path(self, project_id: str) -> pathlib.Path:
        return self.root / f"{self._safe_project_id(project_id)}.jsonl"

    def _lock_path(self, project_id: str) -> pathlib.Path:
        return self.root / f"{self._safe_project_id(project_id)}.lock"

    def _safe_project_id(self, project_id: str) -> str:
        if not _PROJECT_ID.fullmatch(project_id):
            raise ValueError(f"invalid project id {project_id!r}")
        return project_id

    @contextlib.contextmanager
    def _locked(self, project_id: str):  # noqa: ANN202
        self.root.mkdir(parents=True, exist_ok=True)
        with self._lock_path(project_id).open("a+", encoding="utf-8") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _redacted(value: Any) -> Any:
    if isinstance(value, str):
        return redact(value)
    if isinstance(value, dict):
        return {redact(str(key)): _redacted(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_redacted(item) for item in value]
    if isinstance(value, tuple):
        return [_redacted(item) for item in value]
    return value
