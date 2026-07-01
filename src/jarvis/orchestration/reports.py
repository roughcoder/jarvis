from __future__ import annotations

from typing import Any

from jarvis.orchestration.models import RunEvent
from jarvis.orchestration.redaction import public_url as _public_url
from jarvis.orchestration.redaction import redact as _redact
from jarvis.orchestration.store import OrchestrationStore


def build_run_report(store: OrchestrationStore, run_id: str) -> dict[str, Any]:
    run = store.get(run_id)
    if run is None:
        raise KeyError(run_id)
    events = store.events(run.run_id)
    return {
        "run_id": run.run_id,
        "objective": _redact(run.objective),
        "phase": run.phase,
        "status": run.status,
        "terminal_reason": _redact(run.terminal_reason),
        "work_items": [
            {
                "source": link.item.source,
                "id": link.item.id,
                "kind": link.item.kind,
                "title": _redact(link.item.title),
                "url": _public_url(link.item.url),
                "role": link.role,
            }
            for link in run.work_items
        ],
        "sessions": [
            {
                "worker_id": session.worker_id,
                "session_id": session.session_id,
                "status": session.status,
                "provider": session.provider,
                "engine": session.engine,
                "branch": session.branch,
            }
            for session in run.sessions
        ],
        "artifacts": [
            {
                "type": artifact.type,
                "id": artifact.id,
                "url": _public_url(artifact.url),
                "name": _redact(artifact.name),
                "status": artifact.status,
            }
            for artifact in run.artifacts
            if artifact.public
        ],
        "events": [_event_summary(event) for event in events[-20:]],
        "public_safe": True,
    }


def format_run_report(report: dict[str, Any]) -> str:
    lines = [
        f"Run: {report.get('run_id', '')}",
        f"Objective: {report.get('objective', '')}",
        f"Status: {report.get('phase', '')} ({report.get('status', '')})",
    ]
    if report.get("terminal_reason"):
        lines.append(f"Result: {report['terminal_reason']}")
    if report.get("sessions"):
        lines.append("Sessions:")
        lines.extend(
            f"  - {session['worker_id']}:{session['session_id']} {session['status']} {session['provider']}/{session['engine']} {session.get('branch', '')}".rstrip()
            for session in report["sessions"]
        )
    if report.get("artifacts"):
        lines.append("Artifacts:")
        lines.extend(
            f"  - {artifact['type']}: {artifact.get('url') or artifact.get('name') or artifact.get('id')}".rstrip()
            for artifact in report["artifacts"]
        )
    if report.get("events"):
        lines.append("Recent events:")
        lines.extend(f"  - {event['type']}: {event['message']}".rstrip() for event in report["events"])
    return "\n".join(lines)


def public_status_comment(report: dict[str, Any]) -> str:
    lines = [
        "Jarvis run report",
        f"- Run: {report.get('run_id', '')}",
        f"- Status: {report.get('phase', '')}",
    ]
    artifacts = [artifact for artifact in report.get("artifacts", []) if artifact.get("url")]
    if artifacts:
        lines.append("- Artifacts:")
        lines.extend(f"  - {artifact['type']}: {artifact['url']}" for artifact in artifacts)
    if report.get("terminal_reason"):
        lines.append(f"- Result: {report['terminal_reason']}")
    return "\n".join(lines)


def _event_summary(event: RunEvent) -> dict[str, str]:
    return {
        "type": event.type,
        "message": _redact(event.message),
        "time": event.time,
    }
