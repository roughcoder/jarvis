# Household Email and Calendar

This is the build spec for Jarvis managing its own email/calendar and, with
explicit consent, the household's email and calendars.

The tool surface must stay provider-neutral. Jarvis should offer actions such as
`search_email`, `upcoming_events`, `send_email`, `create_calendar_invite`, and
`respond_to_calendar_invite`; it should not offer "Gmail" or "Microsoft Graph"
as model-visible tools. Google, Google Workspace, Microsoft 365, and future
providers live behind adapters.

## Goals

- Jarvis can manage the house account: send email, send invites, read house
  calendar events, and receive replies.
- A person can grant calendar access in one of two normal modes:
  - `calendar.viewer`: Jarvis can read availability/events and sends invites
    from the house account.
  - `calendar.delegate`: Jarvis can create/update/delete events and RSVP as that
    person, subject to the policy gates below.
- A person may optionally grant email access:
  - `email.reader`: search, summarize, classify, and propose cleanup.
  - `email.delegate`: draft replies, send explicitly approved replies, and
    archive/label/delete under the cleanup policy.
- Onboarding can be driven over WhatsApp: Jarvis explains the options, walks the
  user through OAuth or calendar sharing, verifies the grant, creates a reversible
  test event or invite, then records the account binding.
- Jarvis can answer "what is Alice doing today?" only when the asker has a grant
  that allows seeing Alice's data, or when Alice's grant explicitly allows
  household-visible summaries.

## Non-Goals

- Do not put provider OAuth tokens in tracked files, prompts, user profile
  markdown, or tool results.
- Do not build a separate provider-specific tool per account type.
- Do not rely on browser automation for normal mail/calendar access. Browser
  control remains a fallback for unsupported providers, not the primary API.
- Do not let a shared room device or WhatsApp channel inherit full delegated
  write authority just because the provider token exists.

## External Patterns to Adopt

OpenClaw's useful pattern is channel-first onboarding with deny-by-default remote
DM access: unknown senders enter pairing instead of driving the assistant, and an
operator approves them before their messages are processed. Jarvis already has
the matching WhatsApp pairing surface; household account onboarding should extend
that flow rather than creating a second invitation mechanism.

Hermes' useful MCP guidance is "connect the smallest useful surface." For
Jarvis, that means provider adapters should expose a narrow domain interface,
not raw provider APIs or a large MCP server surface. MCP remains acceptable for
future providers, but any MCP-backed account must still pass through the same
domain policy layer.

OpenClaw's `gogcli` is the right first Google adapter rather than a temporary
hack. It already gives Jarvis the properties this design needs: account aliases,
stable JSON/plain output, a machine-readable schema, non-interactive execution,
exact command allow-lists, and safety switches such as blocking Gmail sends.
Jarvis should keep wrapping it behind the provider-neutral email/calendar tools
instead of importing Google SDKs directly in the brain.

OpenClaw and Hermes both separate model/provider/runtime concerns from user
workflows. Jarvis should keep that separation: the model asks for an email or
calendar action; the account router chooses a principal, provider adapter, and
credential binding.

## Principals and Accounts

Jarvis has three kinds of actors:

- `house`: Jarvis's own account. This is the default for unknown speakers and
  for invite-sending when a person grants read-only calendar access.
- `person`: a named human from `jarvis-workspace/users/<name>.md`.
- `household`: a policy group, not a credential owner. It can receive summaries
  only when individual people opt in.

User profile front matter may contain account binding references, never tokens:

```yaml
calendar_accounts: [alice-primary-calendar]
email_accounts: [alice-primary-mail]
household_visibility: availability
capabilities: [calendar.freebusy, calendar.read]
```

The binding names resolve to private state under an ignored account store, for
example `jarvis-workspace/.accounts/<principal>/<binding>.json`, or a later
Keychain/vault backend. The public profile only says which bindings exist and
which Jarvis capabilities the user grants.

## Capability Model

Capabilities are domain actions. Provider scopes are adapter implementation
details.

| Capability | Meaning | Default policy |
| --- | --- | --- |
| `calendar.freebusy` | Read busy/free windows without event details | Allowed for household scheduling when granted |
| `calendar.read` | Read event titles, attendees, location, and notes | Personal only unless explicitly household-visible |
| `calendar.invite` | Create an event on the house calendar and invite others | Draft/confirm on shared or remote channels |
| `calendar.write` | Create/update/delete events on the person's own calendar | Strong identity plus grant; confirm destructive edits |
| `calendar.rsvp` | Accept/decline/tentatively respond to invites | Strong identity plus grant; confirm non-obvious RSVP |
| `email.read` | Search/read mailbox content | Personal only |
| `email.draft` | Compose a draft reply without sending | Allowed with `email.read`; safer default |
| `email.send` | Send email from an account | Confirm unless pre-granted for the channel and recipient class |
| `email.modify` | Archive, label, mark read/unread | Confirm batches; reversible actions can be pre-granted |
| `email.delete` | Trash/delete messages | Always confirm; permanent delete is out of scope for v1 |

The current Google-backed house account tool already uses `email.read`,
`email.send`, and `calendar.read`; future provider work should extend this
matrix rather than adding provider-named capabilities.

## Policy Gates

Every account action is decided from:

- principal: house or named person
- channel: voice, WhatsApp, text console, background job
- identity confidence: strong, claimed, unknown
- account grant: freebusy, read, delegate, or email delegate
- action risk: read, reversible write, external send, destructive write
- recipient/attendee class: self, household member, known contact, external
- execution mode: immediate, draft, confirm, or deny

Default rules:

- Unknown identity can use only the house principal.
- Claimed identity on a shared room device can read free/busy if granted, but
  should not see private event details or send from a personal account.
- Strong identity can use that person's grants.
- WhatsApp is a higher-risk remote channel. Writes from WhatsApp default to
  draft/confirm unless the user explicitly pre-grants autonomous action for a
  narrow class such as "send calendar invites from the house account."
- Email sending, RSVP decisions, deleting messages, and editing somebody else's
  calendar require a clear actor and an audit entry.
- Background jobs inherit the asker's exact capabilities and never gain new
  account authority.

Audit events should include the actor, requester, channel, account binding,
provider adapter, operation, target ids, confirmation mode, and result. They
must not include OAuth tokens or full email bodies.

## Onboarding Over WhatsApp

The WhatsApp connector already handles remote pairing. Account onboarding should
start only after pairing resolves the sender to a known person.

1. User sends "connect my calendar" or "let Jarvis manage my email."
2. Jarvis explains the available grant modes in plain language:
   - read availability only
   - read calendar details
   - send invites from Jarvis's house account
   - manage my calendar
   - read/draft/manage my email
3. Jarvis sends a setup link or instructions:
   - OAuth for delegated Google/Microsoft access.
   - Calendar sharing to the house account for read-only or availability-first
     access, where the provider supports it.
4. The account service verifies the grant with a narrow probe:
   - calendar: read "now to seven days" or free/busy.
   - write calendar: create a clearly named temporary test event, verify it,
     then delete/cancel it.
   - house invite mode: create a test invite from the house account to the
     person's email, ask them to confirm it arrived, then cancel it.
   - email: search for a harmless self-authored setup message or send a test
     message to the house account.
5. Jarvis records only the binding name and granted capabilities in the user
   profile; tokens remain in the private account store.
6. Jarvis reports exactly what it can now do and how to revoke it.

Revocation should be a first-class flow: "disconnect my calendar" removes the
binding and tells the person how to revoke provider-side OAuth or calendar
sharing.

## Provider Adapter Interface

The account router chooses an adapter by binding metadata, but the tools call a
domain interface:

```python
class CalendarAdapter:
    async def freebusy(principal, start, end): ...
    async def list_events(principal, start, end, detail_level): ...
    async def create_event(principal, event, *, send_updates): ...
    async def update_event(principal, event_id, patch, *, send_updates): ...
    async def delete_event(principal, event_id, *, send_updates): ...
    async def respond_to_invite(principal, event_id, response): ...

class EmailAdapter:
    async def search(principal, query, *, max_results): ...
    async def get_message(principal, message_id, *, body_mode): ...
    async def create_draft(principal, draft): ...
    async def send(principal, message): ...
    async def modify(principal, message_ids, labels, flags): ...
```

Provider mappings:

- Google/Gmail/Workspace:
  - Use `openclaw/gogcli` as the first adapter implementation. Invoke it with
    non-interactive, stable-output flags and the smallest exact command
    allow-list for the domain operation being performed.
  - Calendar event writes use Calendar API events with `sendUpdates` when
    attendees must be notified.
  - Mail send should request the narrow Gmail send scope where possible.
  - Mail cleanup needs broader Gmail modify/read scopes and should be opt-in.
- Microsoft 365/Outlook:
  - Calendar event creation uses Graph calendar event APIs with
    `Calendars.ReadWrite`.
  - Mail send uses Graph `Mail.Send`.
  - Mail cleanup uses `Mail.ReadWrite` and should stay behind `email.modify`.

OAuth should use least privilege per grant mode. The account router may require
re-authentication when a person upgrades from read-only to write access; it
should not silently expand scopes.

## Tool Surface

Keep the visible tools few and stable:

- `search_email`
- `summarize_email_thread`
- `draft_email_reply`
- `send_email`
- `upcoming_events`
- `calendar_freebusy`
- `create_calendar_invite`
- `update_calendar_event`
- `respond_to_calendar_invite`

Provider-specific setup commands can exist as CLI/admin commands, but the model
tool names and capabilities should remain provider-neutral.

## Implementation Plan

1. Finish the provider-neutral capability rename for the existing house account
   adapter. Keep `openclaw/gogcli` as the Google implementation detail.
2. Add an account binding store and parser for user profile binding references.
3. Add an account policy evaluator that returns `allow`, `draft`, `confirm`, or
   `deny`, with unit tests for every row in the capability matrix.
4. Introduce `CalendarAdapter` and `EmailAdapter` protocols plus a fake adapter
   for hermetic tests.
5. Port the current `gogcli` operations into the adapter shape for the house
   account.
6. Add onboarding state for WhatsApp: pending grant, provider/setup method,
   verification probe, success, revocation.
7. Add Google and Microsoft delegated adapters with least-privilege OAuth grant
   modes.
8. Add email cleanup and RSVP flows only after confirmation/audit logging is in
   place.

## Verification

- Unit-test capability gating for every tool.
- Unit-test policy decisions across principal, channel, confidence, grant, and
  action risk.
- Use fake provider adapters for tool and onboarding tests.
- Keep live provider tests opt-in and self-skipping without credentials.
- Verify public readiness so no tokens, mailbox content, calendar ids, or real
  household details land in tracked files.
