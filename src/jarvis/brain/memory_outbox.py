"""Durable Lane 2 curation outbox.

In-turn curation writes append one fsync'd JSONL event. Delivery to the memory
backend happens later through `flush()`, with idempotency by content hash.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from jarvis.brain._storage import atomic_write_json
from jarvis.brain.memory_client import MemoryBackend


OutboxOperation = Literal["create_conclusion", "delete_conclusion"]
OutboxStatus = Literal["pending", "delivered", "failed", "cancelled"]


@dataclass(frozen=True)
class ActiveRetraction:
    observed_id: str
    retracted_content: str
    retracted_conclusion_id: str
    retracted_conclusion_level: str = ""
    retraction_reason: str = ""
    recorded_by: str = ""
    observed_at: str = ""
    metadata: dict[str, Any] | None = None
    created_at: float = 0.0
    updated_at: float = 0.0


@dataclass(frozen=True)
class OutboxEntry:
    id: str
    operation: OutboxOperation
    status: OutboxStatus
    observed_id: str
    content: str = ""
    observer_id: str = ""
    conclusion_id: str = ""
    metadata: dict[str, Any] | None = None
    attempts: int = 0
    content_hash: str = ""
    error: str = ""
    created_at: float = 0.0
    updated_at: float = 0.0


class RetractionIndex:
    """Durable local index of active memory retractions.

    Honcho v3.0.11 does not expose Jarvis's contradiction metadata to the
    dreamer/dialectic path, so this local file is the answer-time suppression
    source of truth. Reads are pure local file reads and are safe on the voice
    hot path.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser()
        self._data: dict[str, Any] | None = None
        self._file_state: tuple[int, int] | None = None

    def record(self, *, observed_id: str, metadata: dict[str, Any]) -> ActiveRetraction | None:
        retracted_content = str(metadata.get("retracted_content") or "").strip()
        retracted_conclusion_id = str(metadata.get("retracted_conclusion_id") or "").strip()
        if not observed_id or not retracted_content:
            return None
        key = retracted_conclusion_id or conclusion_content_hash(
            observed_id=observed_id,
            content=retracted_content,
            observer_id=str(metadata.get("recorded_by") or ""),
            metadata=metadata,
        )
        now = time.time()
        data = self._read()
        peers = data.setdefault("peers", {})
        peer_rows = peers.setdefault(observed_id, {})
        existing = peer_rows.get(key) if isinstance(peer_rows.get(key), dict) else {}
        row = {
            "observed_id": observed_id,
            "retracted_content": retracted_content,
            "retracted_conclusion_id": retracted_conclusion_id,
            "retracted_conclusion_level": str(metadata.get("retracted_conclusion_level") or ""),
            "retraction_reason": str(metadata.get("retraction_reason") or ""),
            "recorded_by": str(metadata.get("recorded_by") or ""),
            "observed_at": str(metadata.get("observed_at") or ""),
            "metadata": dict(metadata),
            "created_at": float(existing.get("created_at", now)),
            "updated_at": now,
        }
        peer_rows[key] = row
        atomic_write_json(self.path, data)
        self._cache_data(data)
        return _retraction_from_json(row)

    def clear_for_assertion(self, *, observed_id: str, content: str) -> list[ActiveRetraction]:
        content = content.strip()
        if not observed_id or not content:
            return []
        data = self._read()
        peer_rows = data.get("peers", {}).get(observed_id)
        if not isinstance(peer_rows, dict):
            return []
        removed: list[ActiveRetraction] = []
        for key, raw in list(peer_rows.items()):
            if not isinstance(raw, dict):
                continue
            record = _retraction_from_json(raw)
            if _text_matches(record.retracted_content, content):
                removed.append(record)
                peer_rows.pop(key, None)
        if not removed:
            return []
        if not peer_rows:
            data.get("peers", {}).pop(observed_id, None)
        if not data.get("peers"):
            data.pop("peers", None)
        atomic_write_json(self.path, data)
        self._cache_data(data)
        return removed

    def active(self, *, observed_id: str) -> list[ActiveRetraction]:
        if not observed_id:
            return []
        peer_rows = self._read().get("peers", {}).get(observed_id)
        if not isinstance(peer_rows, dict):
            return []
        records = [
            _retraction_from_json(raw)
            for raw in peer_rows.values()
            if isinstance(raw, dict)
        ]
        return sorted(records, key=lambda row: (row.observed_at, row.created_at, row.retracted_content))

    def _read(self) -> dict[str, Any]:
        file_state = self._current_file_state()
        if self._data is not None and self._file_state == file_state:
            return self._data
        if file_state == (0, 0):
            self._data = {"peers": {}}
            self._file_state = file_state
            return {"peers": {}}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            raw = {"peers": {}}
        if not isinstance(raw, dict):
            raw = {"peers": {}}
        peers = raw.get("peers")
        if not isinstance(peers, dict):
            raw["peers"] = {}
        self._data = raw
        self._file_state = file_state
        return raw

    def _current_file_state(self) -> tuple[int, int]:
        try:
            stat = self.path.stat()
        except OSError:
            return (0, 0)
        return (stat.st_size, stat.st_mtime_ns)

    def _cache_data(self, data: dict[str, Any]) -> None:
        self._data = data
        self._file_state = self._current_file_state()


def retraction_index_path(curation_outbox_path: str | Path) -> Path:
    path = Path(curation_outbox_path).expanduser()
    if path.suffix:
        return path.with_suffix(".retractions.json")
    return path.with_name(f"{path.name}.retractions.json")


class CurationOutbox:
    def __init__(
        self,
        path: str | Path,
        *,
        max_retries: int = 3,
        backoff_initial_s: float = 0.25,
        backoff_max_s: float = 2.0,
    ) -> None:
        self.path = Path(path).expanduser()
        self.max_retries = max(1, max_retries)
        self.backoff_initial_s = max(0.0, backoff_initial_s)
        self.backoff_max_s = max(self.backoff_initial_s, backoff_max_s)
        self._entries: dict[str, OutboxEntry] | None = None
        self._journal_size: int | None = None

    def enqueue_create(
        self,
        *,
        observed_id: str,
        content: str,
        observer_id: str,
        metadata: dict[str, Any],
    ) -> OutboxEntry:
        metadata = dict(metadata)
        content_hash = metadata.get("content_hash") or conclusion_content_hash(
            observed_id=observed_id,
            content=content,
            observer_id=observer_id,
            metadata=metadata,
        )
        metadata["content_hash"] = content_hash
        now = time.time()
        entry = OutboxEntry(
            id=str(uuid.uuid4()),
            operation="create_conclusion",
            status="pending",
            observed_id=observed_id,
            content=content,
            observer_id=observer_id,
            metadata=metadata,
            content_hash=content_hash,
            created_at=now,
            updated_at=now,
        )
        self._append_event({"event": "queued", **_entry_to_json(entry)})
        return entry

    def enqueue_delete(
        self,
        *,
        conclusion_id: str,
        observed_id: str = "",
        content: str = "",
        observer_id: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> OutboxEntry:
        now = time.time()
        entry = OutboxEntry(
            id=str(uuid.uuid4()),
            operation="delete_conclusion",
            status="pending",
            observed_id=observed_id,
            content=content,
            observer_id=observer_id,
            conclusion_id=conclusion_id,
            metadata=dict(metadata or {}),
            content_hash=f"delete:{conclusion_id}",
            created_at=now,
            updated_at=now,
        )
        self._append_event({"event": "queued", **_entry_to_json(entry)})
        return entry

    def cancel_pending(self, *, observed_id: str, content: str) -> list[OutboxEntry]:
        """Append cancellation records for pending creates matching the visible pending line."""
        normalized = content.strip()
        if not normalized:
            return []
        matches = [
            entry
            for entry in self.pending_entries(observed_id=observed_id)
            if entry.operation == "create_conclusion" and _cancel_matches(entry, normalized)
        ]
        now = time.time()
        for entry in matches:
            updated = _replace_entry(entry, status="cancelled", updated_at=now, error="")
            self._append_event({"event": "cancelled", **_entry_to_json(updated)})
        return matches

    async def flush(self, backend: MemoryBackend, *, notify=None) -> dict[str, int]:  # noqa: ANN001
        return await asyncio.to_thread(self.flush_sync, backend, notify=notify)

    def flush_sync(self, backend: MemoryBackend, *, notify=None) -> dict[str, int]:  # noqa: ANN001
        delivered = 0
        failed = 0
        for entry in self.pending_entries():
            current = entry
            while current.attempts < self.max_retries:
                try:
                    self._deliver_one(backend, current)
                except Exception as exc:  # noqa: BLE001 - persisted and retried/reported.
                    current = self._record_attempt(current, exc)
                    if current.attempts >= self.max_retries:
                        self._mark_failed(current, exc)
                        failed += 1
                        if notify is not None:
                            notify("I couldn't save a declared memory item to memory.")
                        break
                    delay = min(
                        self.backoff_max_s,
                        self.backoff_initial_s * (2 ** max(0, current.attempts - 1)),
                    )
                    if delay:
                        time.sleep(delay)
                    continue
                self._mark_delivered(current)
                delivered += 1
                break
        return {"delivered": delivered, "failed": failed}

    def pending_entries(self, *, observed_id: str | None = None) -> list[OutboxEntry]:
        entries = [
            entry for entry in self._current_entries().values()
            if entry.status == "pending"
        ]
        if observed_id:
            entries = [entry for entry in entries if entry.observed_id == observed_id]
        return sorted(entries, key=lambda entry: entry.created_at)

    def pending_lines(self, *, observed_id: str | None = None) -> list[str]:
        lines: list[str] = []
        for entry in self.pending_entries(observed_id=observed_id):
            if entry.operation == "delete_conclusion":
                detail = entry.content or entry.conclusion_id
                lines.append(f"pending, not yet saved: forget {detail}")
            else:
                lines.append(f"pending, not yet saved: {entry.content}")
        return lines

    def append_pending_lines(self, text: str, *, observed_id: str | None = None) -> str:
        lines = self.pending_lines(observed_id=observed_id)
        if not lines:
            return text
        return "\n".join(part for part in (text.strip(), *lines) if part)

    def _deliver_one(self, backend: MemoryBackend, entry: OutboxEntry) -> None:
        if entry.operation == "delete_conclusion":
            backend.delete_conclusion(entry.conclusion_id)
            return
        existing = backend.list_conclusions(
            observed_id=entry.observed_id,
            level="explicit",
        )
        if any(
            record.content == entry.content
            and record.observed_id == entry.observed_id
            and (not entry.observer_id or record.observer_id == entry.observer_id)
            for record in existing
        ):
            return
        backend.create_conclusion(
            observed_id=entry.observed_id,
            observer_id=entry.observer_id or None,
            content=entry.content,
            metadata=dict(entry.metadata or {}),
        )

    def _record_attempt(self, entry: OutboxEntry, exc: Exception) -> OutboxEntry:
        updated = _replace_entry(
            entry,
            attempts=entry.attempts + 1,
            error=str(exc),
            updated_at=time.time(),
        )
        self._append_event({"event": "attempt", **_entry_to_json(updated)})
        return updated

    def _mark_delivered(self, entry: OutboxEntry) -> None:
        updated = _replace_entry(entry, status="delivered", updated_at=time.time(), error="")
        self._append_event({"event": "delivered", **_entry_to_json(updated)})

    def _mark_failed(self, entry: OutboxEntry, exc: Exception) -> None:
        updated = _replace_entry(entry, status="failed", error=str(exc), updated_at=time.time())
        self._append_event({"event": "failed", **_entry_to_json(updated)})

    def _current_entries(self) -> dict[str, OutboxEntry]:
        self._load_entries()
        return dict(self._entries or {})

    def _append_event(self, event: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._load_entries()
        payload = json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n"
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            dir_fd = os.open(self.path.parent, os.O_DIRECTORY)
        except OSError:
            return
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        self._apply_event(event)
        self._journal_size = self.path.stat().st_size
        self._write_state()

    @property
    def _state_path(self) -> Path:
        return self.path.with_name(f"{self.path.name}.pending.json")

    def _load_entries(self) -> None:
        journal_size = self.path.stat().st_size if self.path.exists() else 0
        if self._entries is not None and self._journal_size == journal_size:
            return
        loaded = self._load_state(journal_size)
        if loaded is not None:
            self._entries = loaded
            self._journal_size = journal_size
            return
        self._entries = {}
        if self.path.exists():
            for line in self.path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                self._apply_event(json.loads(line))
        self._journal_size = journal_size
        self._write_state()

    def _load_state(self, journal_size: int) -> dict[str, OutboxEntry] | None:
        if not self._state_path.exists():
            return None
        try:
            raw = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None
        if int(raw.get("journal_size", -1)) != journal_size:
            return None
        entries = raw.get("entries")
        if not isinstance(entries, list):
            return None
        return {entry.id: entry for entry in (_entry_from_json(item) for item in entries) if entry.status == "pending"}

    def _write_state(self) -> None:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "journal_size": self._journal_size or 0,
            "entries": [_entry_to_json(entry) for entry in (self._entries or {}).values()],
        }
        tmp = self._state_path.with_name(f".{self._state_path.name}.tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
        os.replace(tmp, self._state_path)

    def _apply_event(self, raw: dict[str, Any]) -> None:
        if self._entries is None:
            self._entries = {}
        entry = _entry_from_json(raw)
        if entry.status == "pending":
            self._entries[entry.id] = entry
        else:
            self._entries.pop(entry.id, None)


def conclusion_content_hash(
    *,
    observed_id: str,
    content: str,
    observer_id: str,
    metadata: dict[str, Any],
) -> str:
    payload = {
        "observed_id": observed_id,
        "content": content,
        "observer_id": observer_id,
        "metadata": {k: v for k, v in sorted(metadata.items()) if k != "content_hash"},
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _entry_to_json(entry: OutboxEntry) -> dict[str, Any]:
    return {
        "id": entry.id,
        "operation": entry.operation,
        "status": entry.status,
        "observed_id": entry.observed_id,
        "content": entry.content,
        "observer_id": entry.observer_id,
        "conclusion_id": entry.conclusion_id,
        "metadata": dict(entry.metadata or {}),
        "attempts": entry.attempts,
        "content_hash": entry.content_hash,
        "error": entry.error,
        "created_at": entry.created_at,
        "updated_at": entry.updated_at,
    }


def _entry_from_json(raw: dict[str, Any]) -> OutboxEntry:
    return OutboxEntry(
        id=str(raw.get("id", "")),
        operation=str(raw.get("operation", "create_conclusion")),  # type: ignore[arg-type]
        status=str(raw.get("status", "pending")),  # type: ignore[arg-type]
        observed_id=str(raw.get("observed_id", "")),
        content=str(raw.get("content", "")),
        observer_id=str(raw.get("observer_id", "")),
        conclusion_id=str(raw.get("conclusion_id", "")),
        metadata=dict(raw.get("metadata") or {}),
        attempts=int(raw.get("attempts", 0)),
        content_hash=str(raw.get("content_hash", "")),
        error=str(raw.get("error", "")),
        created_at=float(raw.get("created_at", 0.0)),
        updated_at=float(raw.get("updated_at", 0.0)),
    )


def _retraction_from_json(raw: dict[str, Any]) -> ActiveRetraction:
    return ActiveRetraction(
        observed_id=str(raw.get("observed_id", "")),
        retracted_content=str(raw.get("retracted_content", "")),
        retracted_conclusion_id=str(raw.get("retracted_conclusion_id", "")),
        retracted_conclusion_level=str(raw.get("retracted_conclusion_level", "")),
        retraction_reason=str(raw.get("retraction_reason", "")),
        recorded_by=str(raw.get("recorded_by", "")),
        observed_at=str(raw.get("observed_at", "")),
        metadata=dict(raw.get("metadata") or {}),
        created_at=float(raw.get("created_at", 0.0)),
        updated_at=float(raw.get("updated_at", 0.0)),
    )


def _cancel_matches(entry: OutboxEntry, query: str) -> bool:
    if query == entry.content_hash or query == str((entry.metadata or {}).get("content_hash", "")):
        return True
    entry_text = " ".join(entry.content.split())
    query_text = " ".join(query.split())
    if not entry_text or not query_text:
        return False
    entry_folded = entry_text.casefold()
    query_folded = query_text.casefold()
    return entry_folded == query_folded or query_folded in entry_folded or entry_folded in query_folded


_MIN_CONTAINMENT_CHARS = 24
_MIN_CONTAINMENT_TOKENS = 4
_PUNCTUATION_RE = re.compile(r"[^\w\s]+", re.UNICODE)


def memory_text_matches(left: str, right: str) -> bool:
    """Match exact normalized claims, or substantial contained restatements."""
    left_text = _normalise_claim_text(left)
    right_text = _normalise_claim_text(right)
    if not left_text or not right_text:
        return False
    if left_text == right_text:
        return True
    shorter, longer = sorted((left_text, right_text), key=len)
    if len(shorter) < _MIN_CONTAINMENT_CHARS:
        return False
    if len(shorter.split()) < _MIN_CONTAINMENT_TOKENS:
        return False
    return shorter in longer


def _text_matches(left: str, right: str) -> bool:
    return memory_text_matches(left, right)


def _normalise_claim_text(value: str) -> str:
    without_punctuation = _PUNCTUATION_RE.sub(" ", value.casefold())
    return " ".join(without_punctuation.split())


def _replace_entry(entry: OutboxEntry, **changes: Any) -> OutboxEntry:
    data = _entry_to_json(entry)
    data.update(changes)
    return _entry_from_json(data)
