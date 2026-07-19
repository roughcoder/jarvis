from __future__ import annotations

from typing import Any

import pytest

from jarvis.config import WorkerConfig
from jarvis.orchestration.cockpit import (
    WorkerReadDiagnostic,
    _worker_bulk_checkpoint_read,
    _worker_collection_read,
    aggregate_requests,
)


class Response:
    def __init__(self, body: Any, *, status_code: int = 200) -> None:
        self._body = body
        self.status_code = status_code

    def json(self) -> Any:
        return self._body


@pytest.mark.parametrize(
    ("resource", "collection_key"),
    [("sessions", "sessions"), ("requests", "requests"), ("session_checkpoints", "checkpoints")],
)
def test_worker_collection_read_distinguishes_successful_empty_from_failure(
    resource: str,
    collection_key: str,
) -> None:
    successful = _worker_collection_read(
        worker_id="worker-1",
        resource=resource,
        url=f"http://worker.test/{resource}",
        collection_key=collection_key,
        headers={},
        timeout=1.0,
        http_get=lambda *_args, **_kwargs: Response({collection_key: []}),
    )

    def fail(*_args: Any, **_kwargs: Any) -> Response:
        raise TimeoutError("private worker address must not enter diagnostics")

    failed = _worker_collection_read(
        worker_id="worker-1",
        resource=resource,
        url=f"http://worker.test/{resource}",
        collection_key=collection_key,
        headers={},
        timeout=1.0,
        http_get=fail,
    )

    assert successful.status == "success"
    assert successful.items == ()
    assert successful.diagnostic is None
    assert failed.status == "failure"
    assert failed.items == ()
    assert failed.diagnostic == WorkerReadDiagnostic(
        worker_id="worker-1",
        resource=resource,
        status="failure",
        failure_kind="transport_error",
        error_type="TimeoutError",
    )


def test_worker_bulk_checkpoint_read_distinguishes_unsupported_from_failure() -> None:
    unsupported = _worker_bulk_checkpoint_read(
        "worker-1",
        "http://worker.test",
        {},
        1.0,
        lambda *_args, **_kwargs: Response({}, status_code=404),
    )
    failed = _worker_bulk_checkpoint_read(
        "worker-1",
        "http://worker.test",
        {},
        1.0,
        lambda *_args, **_kwargs: Response({}, status_code=503),
    )

    assert unsupported.status == "unsupported"
    assert unsupported.diagnostic is not None
    assert unsupported.diagnostic.status_code == 404
    assert failed.status == "failure"
    assert failed.diagnostic is not None
    assert failed.diagnostic.failure_kind == "http_error"
    assert failed.diagnostic.status_code == 503


def test_request_aggregation_keeps_legacy_empty_result_with_explicit_diagnostics() -> None:
    cfg = WorkerConfig(_env_file=None)
    successful_diagnostics: list[WorkerReadDiagnostic] = []
    failed_diagnostics: list[WorkerReadDiagnostic] = []

    successful = aggregate_requests(
        worker_cfg=cfg,
        workers_path="",
        http_get=lambda *_args, **_kwargs: Response({"requests": []}),
        diagnostics=successful_diagnostics,
    )

    def fail(*_args: Any, **_kwargs: Any) -> Response:
        raise TimeoutError("worker timed out")

    failed = aggregate_requests(
        worker_cfg=cfg,
        workers_path="",
        http_get=fail,
        diagnostics=failed_diagnostics,
    )

    assert successful == failed == []
    assert successful_diagnostics == []
    assert len(failed_diagnostics) == 1
    assert failed_diagnostics[0].status == "failure"
    assert failed_diagnostics[0].resource == "requests"
