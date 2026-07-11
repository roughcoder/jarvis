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


def test_orchestrator_grant_uses_persisted_secret_without_api_token(tmp_path) -> None:  # noqa: ANN001
    cfg = OrchestrationConfig(_env_file=None, api_token="", workspace=str(tmp_path))
    requester = RequestContext(
        "cockpit",
        "neil",
        "personal",
        frozenset(),
        channel="automation",
    )

    token = mint_orchestrator_grant(
        cfg,
        project_id="project_a",
        thread_id="thread_a",
        requester=requester,
    )
    grant = resolve_orchestrator_grant(cfg, token)

    key_file = tmp_path / ".orchestrator-grant-signing-key"
    assert grant.requester.channel == "automation"
    assert key_file.exists()
    assert key_file.stat().st_mode & 0o777 == 0o600


def test_orchestrator_grant_prefers_dedicated_signing_secret() -> None:
    requester = RequestContext("cockpit", "neil", "personal", frozenset())
    first = OrchestrationConfig(_env_file=None, api_token="", grant_signing_secret="first")
    second = OrchestrationConfig(_env_file=None, api_token="", grant_signing_secret="second")
    token = mint_orchestrator_grant(
        first,
        project_id="project_a",
        thread_id="thread_a",
        requester=requester,
    )

    with pytest.raises(OrchestratorGrantError, match="invalid"):
        resolve_orchestrator_grant(second, token)


def test_orchestrator_grant_rejects_symlinked_persisted_key(tmp_path) -> None:  # noqa: ANN001
    target = tmp_path / "unrelated-secret"
    target.write_text("do-not-read")
    key_file = tmp_path / ".orchestrator-grant-signing-key"
    key_file.symlink_to(target)
    cfg = OrchestrationConfig(_env_file=None, api_token="", workspace=str(tmp_path))
    requester = RequestContext("cockpit", "neil", "personal", frozenset())

    with pytest.raises(OrchestratorGrantError, match="unable to read"):
        mint_orchestrator_grant(
            cfg,
            project_id="project_a",
            thread_id="thread_a",
            requester=requester,
        )

    assert target.read_text() == "do-not-read"
