"""Account routing: binding -> policy -> provider adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from jarvis.brain.account_adapters import CalendarAdapter, EmailAdapter
from jarvis.brain.accounts import (
    ALLOW,
    CONFIRM,
    DENY,
    DRAFT,
    AccountBinding,
    AccountPolicyDecision,
    AccountPolicyRequest,
    decide_account_policy,
)
from jarvis.brain.context import RequestContext


@dataclass(frozen=True)
class RoutedAccountDecision:
    binding: AccountBinding
    decision: AccountPolicyDecision


class AccountRouter:
    def __init__(
        self,
        *,
        email_adapters: dict[str, EmailAdapter] | None = None,
        calendar_adapters: dict[str, CalendarAdapter] | None = None,
    ) -> None:
        self._email_adapters = email_adapters or {}
        self._calendar_adapters = calendar_adapters or {}

    def decide(
        self,
        ctx: RequestContext,
        binding: AccountBinding,
        capability: str,
        *,
        recipient_class: str = "external",
        pregranted: bool = False,
        destructive: bool = False,
        obvious: bool = False,
    ) -> RoutedAccountDecision:
        decision = decide_account_policy(
            ctx,
            AccountPolicyRequest(
                target_principal=binding.principal,
                capability=capability,
                account_grants=binding.grants,
                household_visibility=binding.household_visibility,
                recipient_class=recipient_class,
                pregranted=pregranted,
                destructive=destructive,
                obvious=obvious,
            ),
        )
        return RoutedAccountDecision(binding, decision)

    async def search_email(
        self,
        ctx: RequestContext,
        binding: AccountBinding,
        query: str,
        *,
        max_results: int | None = None,
    ) -> str:
        if binding.kind != "email":
            return "error: account binding is not an email account"
        routed = self.decide(ctx, binding, "email.read", recipient_class="self")
        if routed.decision.mode != ALLOW:
            return _policy_text(routed.decision)
        adapter = self._email_adapters.get(binding.provider)
        if adapter is None:
            return f"error: no email adapter registered for provider {binding.provider!r}"
        return await adapter.search(binding, query, max_results=max_results)

    async def send_email(
        self,
        ctx: RequestContext,
        binding: AccountBinding,
        message: dict[str, Any],
        *,
        recipient_class: str = "external",
        pregranted: bool = False,
    ) -> str:
        if binding.kind != "email":
            return "error: account binding is not an email account"
        routed = self.decide(
            ctx,
            binding,
            "email.send",
            recipient_class=recipient_class,
            pregranted=pregranted,
        )
        if routed.decision.mode == ALLOW:
            adapter = self._email_adapters.get(binding.provider)
            if adapter is None:
                return f"error: no email adapter registered for provider {binding.provider!r}"
            return await adapter.send(binding, message)
        if routed.decision.mode == DRAFT:
            adapter = self._email_adapters.get(binding.provider)
            if adapter is None:
                return f"error: no email adapter registered for provider {binding.provider!r}"
            return await adapter.create_draft(binding, message)
        return _policy_text(routed.decision)

    async def list_events(self, ctx: RequestContext, binding: AccountBinding, *, days: int) -> str:
        if binding.kind != "calendar":
            return "error: account binding is not a calendar account"
        routed = self.decide(ctx, binding, "calendar.read", recipient_class="self")
        if routed.decision.mode != ALLOW:
            return _policy_text(routed.decision)
        adapter = self._calendar_adapters.get(binding.provider)
        if adapter is None:
            return f"error: no calendar adapter registered for provider {binding.provider!r}"
        return await adapter.list_events(binding, days=days)


def _policy_text(decision: AccountPolicyDecision) -> str:
    if decision.mode == DENY:
        return f"error: account policy denied this request ({decision.reason})"
    if decision.mode == CONFIRM:
        return f"confirmation required before running this account action ({decision.reason})"
    if decision.mode == DRAFT:
        return f"draft required before running this account action ({decision.reason})"
    return f"error: unsupported account policy decision {decision.mode!r}"
