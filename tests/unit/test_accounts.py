from __future__ import annotations

import json

import pytest

from jarvis.brain.accounts import (
    ALLOW,
    CONFIRM,
    DRAFT,
    DENY,
    AccountBindingError,
    AccountPolicyRequest,
    decide_account_policy,
    load_account_binding,
    load_account_bindings,
)
from jarvis.brain.context import RequestContext
from jarvis.brain.identity import HOUSE


def _ctx(
    *caps: str,
    identity: str = "neil",
    scope: str = "personal",
    channel: str = "voice",
    confidence: str = "strong",
) -> RequestContext:
    return RequestContext(
        "dev",
        identity,
        scope,
        frozenset(caps),
        channel=channel,
        confidence=confidence,
    )


def _req(capability: str, *, target: str = "neil", **kwargs) -> AccountPolicyRequest:  # noqa: ANN003
    return AccountPolicyRequest(
        target_principal=target,
        capability=capability,
        account_grants=frozenset({capability}),
        **kwargs,
    )


def test_load_account_binding_from_private_json(tmp_path) -> None:  # noqa: ANN001
    path = tmp_path / "neil"
    path.mkdir()
    (path / "primary-calendar.json").write_text(
        json.dumps(
            {
                "kind": "calendar",
                "provider": "gogcli",
                "account": "neil",
                "grants": ["calendar.freebusy", "calendar.read"],
                "calendar_id": "primary",
                "credential_ref": "gogcli:neil",
                "household_visibility": "availability",
            }
        )
    )

    binding = load_account_binding(tmp_path, "neil", "primary-calendar")

    assert binding.name == "primary-calendar"
    assert binding.principal == "neil"
    assert binding.kind == "calendar"
    assert binding.provider == "gogcli"
    assert binding.grants == frozenset({"calendar.freebusy", "calendar.read"})
    assert binding.household_visibility == "availability"


def test_load_account_bindings_can_filter_by_kind(tmp_path) -> None:  # noqa: ANN001
    path = tmp_path / "neil"
    path.mkdir()
    (path / "mail.json").write_text(json.dumps({"kind": "email", "provider": "gogcli"}))
    (path / "cal.json").write_text(json.dumps({"kind": "calendar", "provider": "gogcli"}))

    bindings = load_account_bindings(tmp_path, "neil", ["mail", "cal"], kind="calendar")

    assert [binding.name for binding in bindings] == ["cal"]


def test_account_binding_rejects_unsafe_names(tmp_path) -> None:  # noqa: ANN001
    with pytest.raises(AccountBindingError):
        load_account_binding(tmp_path, "../neil", "mail")
    with pytest.raises(AccountBindingError):
        load_account_binding(tmp_path, "neil", "../mail")


def test_account_binding_rejects_token_material(tmp_path) -> None:  # noqa: ANN001
    path = tmp_path / "neil"
    path.mkdir()
    (path / "mail.json").write_text(
        json.dumps({"kind": "email", "provider": "gogcli", "refresh_token": "secret"})
    )

    with pytest.raises(AccountBindingError, match="forbidden secret"):
        load_account_binding(tmp_path, "neil", "mail")


def test_account_binding_rejects_nested_provider_payloads(tmp_path) -> None:  # noqa: ANN001
    path = tmp_path / "neil"
    path.mkdir()
    (path / "mail.json").write_text(
        json.dumps({"kind": "email", "provider": "gogcli", "oauth": {"account": "neil"}})
    )

    with pytest.raises(AccountBindingError, match="flat metadata"):
        load_account_binding(tmp_path, "neil", "mail")


def test_unknown_speaker_can_read_house_calendar_but_cannot_send() -> None:
    ctx = _ctx(
        "calendar.read",
        "email.send",
        identity=HOUSE,
        scope=HOUSE,
        confidence="unknown",
    )

    read = decide_account_policy(ctx, _req("calendar.read", target=HOUSE))
    send = decide_account_policy(ctx, _req("email.send", target=HOUSE))

    assert read.mode == ALLOW
    assert send.mode == DENY


def test_claimed_identity_can_read_own_freebusy_only() -> None:
    ctx = _ctx("calendar.freebusy", "calendar.read", confidence="claimed")

    freebusy = decide_account_policy(ctx, _req("calendar.freebusy"))
    details = decide_account_policy(ctx, _req("calendar.read"))

    assert freebusy.mode == ALLOW
    assert details.mode == DENY


def test_strong_identity_can_read_own_personal_calendar() -> None:
    decision = decide_account_policy(_ctx("calendar.read"), _req("calendar.read"))

    assert decision.mode == ALLOW


def test_whatsapp_write_requires_confirmation_unless_pregranted() -> None:
    ctx = _ctx("email.send", channel="whatsapp")

    needs_confirm = decide_account_policy(
        ctx,
        _req("email.send", recipient_class="household"),
    )
    pregranted = decide_account_policy(
        ctx,
        _req("email.send", recipient_class="household", pregranted=True),
    )

    assert needs_confirm.mode == CONFIRM
    assert pregranted.mode == ALLOW


def test_external_send_requires_confirmation_without_pregrant() -> None:
    ctx = _ctx("calendar.invite")

    decision = decide_account_policy(ctx, _req("calendar.invite", target=HOUSE))

    assert decision.mode == CONFIRM


def test_send_without_send_grant_downgrades_to_draft_when_granted() -> None:
    ctx = _ctx("email.draft")
    req = AccountPolicyRequest(
        target_principal="neil",
        capability="email.send",
        account_grants=frozenset({"email.draft"}),
    )

    decision = decide_account_policy(ctx, req)

    assert decision.mode == DRAFT


def test_other_person_availability_allowed_only_when_visible() -> None:
    ctx = _ctx("calendar.freebusy")

    visible = decide_account_policy(
        ctx,
        _req(
            "calendar.freebusy",
            target="jules",
            household_visibility="availability",
        ),
    )
    private = decide_account_policy(ctx, _req("calendar.freebusy", target="jules"))

    assert visible.mode == ALLOW
    assert private.mode == DENY


def test_missing_account_grant_denies_even_with_context_capability() -> None:
    ctx = _ctx("email.read")
    req = AccountPolicyRequest(
        target_principal="neil",
        capability="email.read",
        account_grants=frozenset(),
    )

    decision = decide_account_policy(ctx, req)

    assert decision.mode == DENY


def test_email_delete_always_confirms() -> None:
    decision = decide_account_policy(_ctx("email.delete"), _req("email.delete"))

    assert decision.mode == CONFIRM
