"""Household account bindings and policy decisions.

Provider credentials live outside the public repo. User profiles only name the
account bindings they have granted; this module resolves private binding
metadata and decides whether an account action is allowed, drafted, confirmed,
or denied.
"""

from __future__ import annotations

import json
import pathlib
import re
from dataclasses import dataclass, field
from typing import Any

from jarvis.brain.context import RequestContext
from jarvis.brain.identity import HOUSE

ALLOW = "allow"
DRAFT = "draft"
CONFIRM = "confirm"
DENY = "deny"

ACCOUNT_KINDS = frozenset({"calendar", "email"})
CONFIDENCE_STRONG = "strong"
CONFIDENCE_CLAIMED = "claimed"
CONFIDENCE_UNKNOWN = "unknown"
HOUSEHOLD_VISIBILITY_AVAILABILITY = "availability"
REMOTE_CHANNELS = frozenset({"whatsapp"})
READ_CAPABILITIES = frozenset({"calendar.freebusy", "calendar.read", "email.read"})
WRITE_CAPABILITIES = frozenset(
    {
        "calendar.invite",
        "calendar.write",
        "calendar.rsvp",
        "email.draft",
        "email.send",
        "email.modify",
        "email.delete",
    }
)
FORBIDDEN_BINDING_KEYS = frozenset(
    {
        "api_key",
        "access_token",
        "client_secret",
        "password",
        "refresh_token",
        "token",
    }
)
FORBIDDEN_BINDING_KEY_SUBSTRINGS = frozenset({"secret"})

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")


class AccountBindingError(ValueError):
    """Raised when private account binding metadata is unsafe or invalid."""


@dataclass(frozen=True)
class AccountBinding:
    name: str
    principal: str
    kind: str
    provider: str
    account: str = ""
    grants: frozenset[str] = field(default_factory=frozenset)
    email: str = ""
    calendar_id: str = ""
    credential_ref: str = ""
    household_visibility: str = ""
    household_recipients: frozenset[str] = field(default_factory=frozenset)
    known_recipients: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class AccountPolicyRequest:
    target_principal: str
    capability: str
    account_grants: frozenset[str] = field(default_factory=frozenset)
    household_visibility: str = ""
    recipient_class: str = "external"  # self | household | known | external
    pregranted: bool = False
    destructive: bool = False
    obvious: bool = False


@dataclass(frozen=True)
class AccountPolicyDecision:
    mode: str
    reason: str = ""

    @property
    def allowed(self) -> bool:
        return self.mode != DENY


def _safe_name(value: str, *, label: str) -> str:
    if not _SAFE_NAME.fullmatch(value or "") or value in {".", ".."}:
        raise AccountBindingError(f"unsafe {label}: {value!r}")
    return value


def _flat_scalar(value: Any, *, key: str) -> str:
    if value is None:
        return ""
    if isinstance(value, bool | int | float | str):
        return str(value)
    raise AccountBindingError(f"binding key {key!r} must be a scalar")


def _string_set(value: Any, *, key: str) -> frozenset[str]:
    if value is None or value == "":
        return frozenset()
    if isinstance(value, str):
        return frozenset(v.strip() for v in value.split(",") if v.strip())
    if isinstance(value, list):
        out: set[str] = set()
        for item in value:
            if not isinstance(item, bool | int | float | str):
                raise AccountBindingError(f"binding key {key!r} must contain scalars")
            item_s = str(item).strip()
            if item_s:
                out.add(item_s)
        return frozenset(out)
    raise AccountBindingError(f"binding key {key!r} must be a string or list")


def parse_account_binding(name: str, principal: str, data: dict[str, Any]) -> AccountBinding:
    """Parse one private binding JSON object.

    The file is metadata only. It can point to a provider account or credential
    alias, but it must not contain OAuth tokens, passwords, API keys, or nested
    provider payloads.
    """
    safe_name = _safe_name(name, label="binding name")
    safe_principal = _safe_name(principal, label="principal")
    if not isinstance(data, dict):
        raise AccountBindingError("binding must be a JSON object")

    _reject_unsafe_binding_shape(data)

    kind = _flat_scalar(data.get("kind"), key="kind")
    if kind not in ACCOUNT_KINDS:
        raise AccountBindingError("binding kind must be 'calendar' or 'email'")

    provider = _flat_scalar(data.get("provider"), key="provider")
    if not provider:
        raise AccountBindingError("binding provider is required")

    file_name = data.get("name")
    if file_name is not None and _flat_scalar(file_name, key="name") != safe_name:
        raise AccountBindingError("binding name does not match file name")

    return AccountBinding(
        name=safe_name,
        principal=safe_principal,
        kind=kind,
        provider=provider,
        account=_flat_scalar(data.get("account"), key="account"),
        grants=_string_set(data.get("grants"), key="grants"),
        email=_flat_scalar(data.get("email"), key="email"),
        calendar_id=_flat_scalar(data.get("calendar_id"), key="calendar_id"),
        credential_ref=_flat_scalar(data.get("credential_ref"), key="credential_ref"),
        household_visibility=_flat_scalar(data.get("household_visibility"), key="household_visibility"),
        household_recipients=_string_set(data.get("household_recipients"), key="household_recipients"),
        known_recipients=_string_set(data.get("known_recipients"), key="known_recipients"),
    )


def load_account_binding(root: str | pathlib.Path, principal: str, name: str) -> AccountBinding:
    safe_principal = _safe_name(principal, label="principal")
    safe_name = _safe_name(name, label="binding name")
    path = pathlib.Path(root) / safe_principal / f"{safe_name}.json"
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise AccountBindingError(f"account binding not found: {safe_principal}/{safe_name}") from exc
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AccountBindingError(f"account binding is not valid JSON: {safe_principal}/{safe_name}") from exc
    return parse_account_binding(safe_name, safe_principal, data)


def _reject_unsafe_binding_shape(data: dict[str, Any]) -> None:
    forbidden: list[str] = []
    for key, value in data.items():
        lowered = str(key).lower()
        if (
            lowered in FORBIDDEN_BINDING_KEYS
            or lowered.endswith("_token")
            or any(fragment in lowered for fragment in FORBIDDEN_BINDING_KEY_SUBSTRINGS)
        ):
            forbidden.append(lowered)
        if isinstance(value, dict):
            raise AccountBindingError(f"binding key {key!r} must be flat metadata")
        if isinstance(value, list):
            for item in value:
                if not isinstance(item, bool | int | float | str):
                    raise AccountBindingError(f"binding key {key!r} must contain scalars")
    if forbidden:
        raise AccountBindingError(f"binding contains forbidden secret key(s): {', '.join(sorted(forbidden))}")


def load_account_bindings(
    root: str | pathlib.Path,
    principal: str,
    names: tuple[str, ...] | list[str],
    *,
    kind: str | None = None,
) -> tuple[AccountBinding, ...]:
    bindings = tuple(load_account_binding(root, principal, name) for name in names)
    if kind is None:
        return bindings
    if kind not in ACCOUNT_KINDS:
        raise AccountBindingError("binding kind must be 'calendar' or 'email'")
    return tuple(binding for binding in bindings if binding.kind == kind)


def decide_account_policy(ctx: RequestContext, req: AccountPolicyRequest) -> AccountPolicyDecision:
    """Decide how an email/calendar account action may execute."""
    if not ctx.can(req.capability):
        if _can_downgrade_send_to_draft(ctx, req):
            return AccountPolicyDecision(DRAFT, "send downgraded to draft")
        return AccountPolicyDecision(DENY, "request context lacks capability")
    if req.capability not in req.account_grants:
        if _can_downgrade_send_to_draft(ctx, req):
            return AccountPolicyDecision(DRAFT, "send downgraded to draft")
        return AccountPolicyDecision(DENY, "account binding does not grant capability")

    if req.capability in READ_CAPABILITIES:
        return _decide_read_policy(ctx, req)
    if req.capability in WRITE_CAPABILITIES:
        return _decide_write_policy(ctx, req)
    return AccountPolicyDecision(DENY, "unknown account capability")


def _decide_read_policy(ctx: RequestContext, req: AccountPolicyRequest) -> AccountPolicyDecision:
    if req.target_principal == HOUSE:
        return AccountPolicyDecision(ALLOW, "house account read")

    if ctx.confidence == CONFIDENCE_UNKNOWN:
        return AccountPolicyDecision(DENY, "unknown speaker cannot read personal accounts")

    if req.target_principal != ctx.identity:
        if (
            req.capability == "calendar.freebusy"
            and req.household_visibility == HOUSEHOLD_VISIBILITY_AVAILABILITY
        ):
            return AccountPolicyDecision(ALLOW, "household availability is visible")
        return AccountPolicyDecision(DENY, "cannot read another person's private account")

    if ctx.confidence == CONFIDENCE_CLAIMED and req.capability != "calendar.freebusy":
        return AccountPolicyDecision(DENY, "claimed identity can only read free/busy")

    return AccountPolicyDecision(ALLOW, "personal read grant")


def _decide_write_policy(ctx: RequestContext, req: AccountPolicyRequest) -> AccountPolicyDecision:
    if req.target_principal != HOUSE:
        if ctx.confidence != CONFIDENCE_STRONG:
            return AccountPolicyDecision(DENY, "personal writes require strong identity")
        if req.target_principal != ctx.identity:
            return AccountPolicyDecision(DENY, "cannot write another person's account")
    elif ctx.confidence == CONFIDENCE_UNKNOWN:
        return AccountPolicyDecision(DENY, "house account writes require a known speaker")

    if req.capability == "email.draft":
        return AccountPolicyDecision(ALLOW, "drafts do not send")
    if req.capability == "email.delete":
        return AccountPolicyDecision(CONFIRM, "email deletion always requires confirmation")
    if req.destructive or req.capability in {"calendar.write", "email.modify"}:
        return AccountPolicyDecision(CONFIRM, "destructive or mailbox-changing action")
    if req.capability == "calendar.rsvp" and not req.obvious:
        return AccountPolicyDecision(CONFIRM, "RSVP needs confirmation")
    if ctx.channel in REMOTE_CHANNELS and not req.pregranted:
        return AccountPolicyDecision(CONFIRM, "remote channel write needs confirmation")
    if req.capability in {"calendar.invite", "email.send"}:
        if req.recipient_class == "external" and not req.pregranted:
            return AccountPolicyDecision(CONFIRM, "external send needs confirmation")
    return AccountPolicyDecision(ALLOW, "write grant")


def _can_downgrade_send_to_draft(ctx: RequestContext, req: AccountPolicyRequest) -> bool:
    if req.capability != "email.send":
        return False
    if not ctx.can("email.draft") or "email.draft" not in req.account_grants:
        return False
    if req.target_principal == HOUSE:
        return ctx.confidence != CONFIDENCE_UNKNOWN
    return req.target_principal == ctx.identity and ctx.confidence != CONFIDENCE_UNKNOWN
