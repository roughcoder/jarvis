"""Jarvis sidecar for Honcho v3 conclusion metadata.

Honcho v3.0.11 accepts extra conclusion metadata in requests but silently drops
it from responses. Lane 2 needs provenance (`recorded_by`, `observed_at`,
`project_id`, and similar fields) to be durable and queryable by Jarvis, so the
v3 client stores that envelope locally by `(workspace, conclusion_id)` and
merges it back into interface records.

Growth is bounded opportunistically: unfiltered workspace conclusion lists
reconcile the sidecar against Honcho's current ids and drop rows for conclusions
that the server has deleted or superseded. Filtered lists and queries do not
prune because they only see a subset.

Upstream issue-shaped note: remove this sidecar only after Honcho conclusion
create/list/query/delete round-trips arbitrary metadata in the API schema.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jarvis.brain._storage import atomic_write_json


_PENDING_KEY = "__pending_by_content_hash__"


class ConclusionMetadataSidecar:
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    def get(self, workspace: str, conclusion_id: str) -> dict[str, Any]:
        return dict(self._read().get(workspace, {}).get(conclusion_id, {}))

    def put(self, workspace: str, conclusion_id: str, metadata: dict[str, Any]) -> None:
        data = self._read()
        workspace_data = data.setdefault(workspace, {})
        workspace_data[conclusion_id] = dict(metadata)
        _delete_pending(workspace_data, metadata.get("content_hash"))
        atomic_write_json(self._path, data)

    def put_pending(
        self,
        workspace: str,
        content_hash: str,
        *,
        observer_id: str,
        observed_id: str,
        content: str,
        metadata: dict[str, Any],
    ) -> None:
        if not content_hash:
            return
        data = self._read()
        workspace_data = data.setdefault(workspace, {})
        pending = workspace_data.setdefault(_PENDING_KEY, {})
        pending[content_hash] = {
            "observer_id": observer_id,
            "observed_id": observed_id,
            "content": content,
            "metadata": dict(metadata),
        }
        atomic_write_json(self._path, data)

    def materialize_pending(
        self,
        workspace: str,
        conclusion_id: str,
        *,
        observer_id: str,
        observed_id: str,
        content: str,
    ) -> dict[str, Any]:
        data = self._read()
        workspace_data = data.get(workspace)
        pending = workspace_data.get(_PENDING_KEY) if workspace_data else None
        if not isinstance(pending, dict):
            return {}
        for content_hash, row in list(pending.items()):
            if not isinstance(row, dict):
                continue
            if (
                row.get("observer_id") == observer_id
                and row.get("observed_id") == observed_id
                and row.get("content") == content
            ):
                metadata = dict(row.get("metadata") or {})
                workspace_data[conclusion_id] = metadata
                pending.pop(content_hash, None)
                if not pending:
                    workspace_data.pop(_PENDING_KEY, None)
                atomic_write_json(self._path, data)
                return metadata
        return {}

    def delete(self, workspace: str, conclusion_id: str) -> None:
        data = self._read()
        workspace_data = data.get(workspace)
        if not workspace_data or conclusion_id not in workspace_data:
            return
        workspace_data.pop(conclusion_id, None)
        if not workspace_data:
            data.pop(workspace, None)
        atomic_write_json(self._path, data)

    def reconcile(self, workspace: str, existing_ids: set[str]) -> None:
        data = self._read()
        workspace_data = data.get(workspace)
        if not workspace_data:
            return
        pruned = {
            cid: meta
            for cid, meta in workspace_data.items()
            if cid == _PENDING_KEY or cid in existing_ids
        }
        if pruned == workspace_data:
            return
        if pruned:
            data[workspace] = pruned
        else:
            data.pop(workspace, None)
        atomic_write_json(self._path, data)

    def _read(self) -> dict[str, dict[str, dict[str, Any]]]:
        if not self._path.exists():
            return {}
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
        if not isinstance(raw, dict):
            return {}
        return raw


def _delete_pending(workspace_data: dict[str, Any], content_hash: Any) -> None:
    if not content_hash:
        return
    pending = workspace_data.get(_PENDING_KEY)
    if not isinstance(pending, dict):
        return
    pending.pop(str(content_hash), None)
    if not pending:
        workspace_data.pop(_PENDING_KEY, None)

