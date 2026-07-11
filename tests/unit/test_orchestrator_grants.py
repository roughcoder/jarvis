from __future__ import annotations

import pytest

from jarvis.config import OrchestrationConfig
from jarvis.orchestration.orchestrator_grants import (
    OrchestratorGrantError,
    mint_orchestrator_grant,
    resolve_orchestrator_grant,
)
from jarvis.runtime import RequestContext


def test_orchestrator_grant_is_thread_scoped_signed_and_expires() -> None:
    cfg = OrchestrationConfig(_env_file=None, api_token="brain-secret")
    requester = RequestContext(
        device_id="cockpit",
        identity="neil",
        scope="personal",
        capabilities=frozenset({"worker.session.create", "orchestration.runs.read"}),
        peer="neil",
    )

    token = mint_orchestrator_grant(
        cfg,
        project_id="project_a",
        thread_id="thread_a",
        requester=requester,
        now=100,
    )
    grant = resolve_orchestrator_grant(cfg, token, now=101)

    assert grant.project_id == "project_a"
    assert grant.thread_id == "thread_a"
    assert grant.requester.identity == "neil"
    assert grant.requester.capabilities == requester.capabilities
    assert "publish_github_pr_review" in grant.tools

    with pytest.raises(OrchestratorGrantError, match="invalid"):
        resolve_orchestrator_grant(
            cfg, token[:-1] + ("A" if token[-1] != "A" else "B"), now=101
        )
    with pytest.raises(OrchestratorGrantError, match="expired"):
        resolve_orchestrator_grant(cfg, token, now=10_000)


def test_orchestrator_grant_requires_api_signing_secret() -> None:
    cfg = OrchestrationConfig(_env_file=None, api_token="")
    requester = RequestContext("cockpit", "neil", "personal", frozenset())

    with pytest.raises(OrchestratorGrantError, match="ORCHESTRATION_API_TOKEN"):
        mint_orchestrator_grant(
            cfg,
            project_id="project_a",
            thread_id="thread_a",
            requester=requester,
        )
