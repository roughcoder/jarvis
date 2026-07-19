# Honcho Memory Model for Jarvis

Date: 2026-07-04 (revised after v3 validation; updated 2026-07-19 for v3-only runtime)

This note records how Jarvis should use Honcho now that Honcho remains the
chosen memory backend. It turns the design discussion into a durable model for
future implementation. This revision was verified against the actual Honcho
v3.0.11 source and API docs (not just the hosted marketing docs) and corrects
the initial draft in three places: message attribution is speaker-only,
curated facts are written as explicit conclusions rather than fabricated
messages, and the two-rail user-memory model collapses into one backend with
two write lanes.

## Source Evidence

Verified at the `v3.0.11` tag of https://github.com/plastic-labs/honcho/
(self-hosted images published at `ghcr.io/plastic-labs/honcho`):

- Workspaces, peers, sessions, and messages work as in the v3 docs. Peers are
  the representation target; a peer can be a person, agent, or any entity.
- **Observer/observed representations are real**: peer-level `observe_me`,
  session-level `observe_others`, and a `target=` parameter on both the
  representation read and the dialectic `chat` endpoint. Reads support
  semantic filtering (`search_query`, `search_top_k`, `max_conclusions`).
- **There is no retroactive reasoning.** Observers only reason over messages
  sent after they joined a session. Set session membership before writing.
- **Conclusions have full CRUD**: agent-authored explicit conclusions can be
  created directly on a peer; conclusions can be listed/queried with filters
  (including `level`: `explicit` / `deductive` / `inductive` /
  `contradiction`) and deleted individually by id.
- **Messages cannot be deleted** (create/get/update only). Sessions can be
  deleted, and conclusions can be filtered by session.
- Per-workspace/peer **deriver custom instructions** (v3.0.7+) let us tell the
  reasoning pipeline how to treat non-conversational peers.
- **Peer cards** (get/set) cache basic biographical grounding per peer.
- File uploads extract text, chunk it, and create messages attributed to the
  supplied peer — the supported ingestion path for documents.

Primary docs: https://honcho.dev/docs/v3/ (architecture, representation,
representation-scopes, storing-data, file-uploads, queue-status).

## Jarvis Vocabulary

| Jarvis concept | Honcho mapping | Rule |
| --- | --- | --- |
| Deployment/environment | Workspace | Hard isolation only. |
| Principal (family member with an account) | Peer (`neil`, `jules`, `alice`) | Speaks; representation derived from their messages. |
| Contact (person we know things about, no account) | Peer (`contact:<id>`) | May speak on strong-identity channels; no query rights. |
| Jarvis assistant | Peer (`jarvis`), `observe_me=false` | Transcript actor only; never a personality source. |
| Project | Entity peer (`project:<id>`) + registry entry | Reasoned memory on the peer; identity/links/ACL in the registry. |
| Conversation/thread/import | Session | Scope transcripts, uploads, and artifact groups. |
| Turn (something someone said) | Message, attributed to the speaker | The transcript lane. |
| Declared fact / finding / decision | Explicit conclusion on the right peer | The curation lane. |
| Long-term ambient memory | Peer representation, read via cold-path cache | Never a live call on the voice hot path. |
| Explicit memory question | Live dialectic/representation query via a gated tool | Allowed mid-turn; the answer is the deliverable. |

"Account" is a Jarvis identity-layer concept (`identity.py`: trust tiers,
devices, channels, capability grants, the right to query memory). Honcho
neither knows nor cares who can log in — peers are just entities. Whether a
peer *speaks* is what differentiates principals from contacts/projects.

## Workspaces

Use workspaces only for hard isolation. Do not create one workspace per user,
project, room, channel, or device.

| Workspace | Purpose |
| --- | --- |
| `jarvis-home` | Real household memory. |
| `jarvis-dev` | Local development and throwaway testing. |
| `jarvis-staging` | Migration and integration-test rehearsal. |
| `jarvis-demo` | Synthetic demo data. |

`MEMORY_WORKSPACE_ID` selects one. Production is `jarvis-home`.

## The Two Write Lanes

This replaces the initial draft's "attribute a message to the entity that
should learn from it" rule, which fought Honcho's model (a message's `peer_id`
means *who said it*; the deriver builds a peer's representation from their own
messages — subject-attributing messages fabricates utterances and pollutes
representations with unattributed hearsay).

### Lane 1 — Transcripts (derived memory)

Every message is attributed to whoever actually said or wrote it. No
exceptions, no per-fact routing.

When Neil says "my sister Sarah lives in Berlin", that message is
`peer_id = neil`, and the deriver concludes into *Neil's* representation that
Neil's sister Sarah lives in Berlin. That is where hearsay belongs: it is
Neil's knowledge, from Neil's perspective, private to Neil's sessions.

Jarvis's replies are written as `peer_id = jarvis` for transcript coherence,
with `observe_me=false` on the jarvis peer (see Configuration) so no
representation is derived from them.

### Lane 2 — Curation (declared memory)

When a fact must land on a *different* entity's representation — "remember
Julia's birthday is in March", "add a finding to the jarvis project",
"remember about Klaus: he's off Fridays" — Jarvis writes an **explicit
conclusion** directly on that peer via the conclusions API
(`level=explicit`), never a fake message.

Declared conclusions are:

- correctly attributed (metadata carries `recorded_by`, `source`);
- immediately queryable (no deriver wait);
- individually deletable (the correction story);
- invisible to transcript-derived reasoning (no pollution).

This lane subsumes the previous two-rail design: the authoritative
`remember`/`forget` rail becomes explicit conclusions in Honcho instead of a
separate personal file. One backend, two write modes: derived vs declared.
Curation writes are capability-gated by `memory.curate` (the successor memory
write lane to profile-file edits). Live memory searches are gated by
`memory.query`; local profile/user file edits remain gated separately by
`profile.write`. All three grants are explicit and deny-by-default.

### Curation durability (never silently lossy, never blocking)

Lane 1 is best-effort — losing one turn's transcript to an outage is a
shrug. Lane 2 is not: a declared fact must be either delivered or reported,
and the guarantee must not slow the turn.

- **In-turn, a Lane 2 write is only a local outbox append** (fsync'd
  journal — microseconds). "Noted" is honest the moment the fact exists
  durably on disk. No network call ever blocks the reply.
- **A background flusher delivers** outbox entries to Honcho (normally
  within milliseconds of the reply) with retry. Every entry carries an
  idempotency key (`content_hash`-based) so a retry after an ambiguous
  timeout cannot double-write.
- **Failure is proactive, not silent**: if the flusher exhausts retries, the
  proactive lane tells the user ("I couldn't save the last two things to
  memory — Honcho has been down since 3pm"). Delivered-or-reported, always.
- **Read-your-writes**: while entries are pending, the memory tool appends a
  plain "pending, not yet saved: …" line from the outbox (including pending
  forgets). No deep merging — the gap is normally milliseconds and only
  visible during an outage.
- Uploads already follow the pattern: the original lands in the Jarvis file
  vault (local, durable) first; only the Honcho ingestion step is queued.

## People

### Principals

Family members with accounts: `neil`, `jules`, `alice`. They authenticate via
devices/channels, hold trust tiers, and can query memory. Their
representations accrue from both lanes: derived (their transcripts) plus
declared (facts recorded about them through curation, e.g. by another family
member with the right capability).

### Contacts

People we know things about who have no account: relatives, bosses,
colleagues, friends, builders, teachers. Contacts are a first-class personal
CRM lane — the household will have many of them.

- **Peer id**: `contact:<id>` (registry-assigned slug; aliases handle speech;
  `identifiers` — phone numbers, later emails — are the dedupe keys).
- **Contacts can speak.** The principal/contact split is about accounts and
  rights (trust tier, capabilities, the right to query memory), not about
  whether they send messages. A contact texting in a family group gets
  Lane 1 like anyone else: messages attributed to their peer,
  `observe_me=true`, a derived representation accruing on top of declared
  facts. Speaking never grants rights: as a requester a contact is guest
  tier — no memory queries, no tools, no curation.
- **Creation is cheap; the bar depends on identity strength.**
  - *Strong identity* (a channel sender with a stable identifier, e.g. a
    WhatsApp number): **auto-create** the contact on first message, keyed by
    identifier, named from the push name. Create a lot of these — messages
    are knowledge.
  - *Weak identity* (a spoken name): never auto-create from mere mentions —
    STT mishears names and names collide. The first "remember about
    Klaus: …" offers creation with disambiguation ("Klaus your boss?").
    Casual mentions are already captured in the speaker's own representation
    by Lane 1 at zero cost.
- **Visibility follows provenance.** Voice-curated contacts default private
  to their creator; auto-created contacts are visible to the participants of
  the thread they first appeared in (family group → household). Widening or
  narrowing is a registry edit by the owner.
- **Duplicates merge shallowly**: re-point identifiers/aliases to the
  surviving registry entry and copy declared conclusions across (Honcho
  peers themselves cannot merge; the abandoned peer is left inert).
- **Peer cards** seed the basics (name, relationship, disambiguators).
- **Cost knob**: every speaking contact adds deriver spend; `observe_me` can
  be switched off per contact if a noisy group makes it wasteful.

If a contact later gets an account (Sarah comes to stay and starts talking to
Jarvis), nothing migrates: the peer id stays, the identity layer starts
mapping her voice/channel to it, and derived memory begins accruing on top of
the declared facts. Promotion is an identity-layer event, not a memory-layer
one.

Contacts live in a registry with the same shape as projects (below): id,
display name, aliases, relationship, owner, visibility, members.

## Projects

Projects are load-bearing: side projects with GitHub repos, Jira boards,
scopes, and specs; kids' projects (a bird-time story); work and household
jobs. A project has three layers:

1. **Registry entry (Jarvis-owned; the system of record for what exists).**
   Identity, aliases, owner, members, visibility, status — plus the `repos`
   list and `links` to the external world (Jira keys, URLs, docs). This is
   what the Cockpit lists, what voice resolves "switch to the bird story project"
   against, and what worker/Jira/GitHub tools use to scope their operations.
   Deliberately not in Honcho: Honcho is a memory, not a database.
2. **Honcho project peer (`project:<id>`) — the reasoned memory.** Findings,
   decisions, and uploaded specs land here as explicit conclusions and
   artifact sessions. "What's the state of the renovation?" is answered here.
3. **External attachments — referenced, never copied.** Repos stay on GitHub,
   tickets in Jira; the registry holds pointers, and memory entries carry
   provenance metadata (`source_url`) back to them.

Registry entry shape:

```json
{
  "id": "jarvis",
  "name": "Jarvis",
  "peer_id": "project:jarvis",
  "aliases": ["the jarvis project", "jarvis"],
  "owner": "neil",
  "members": ["neil"],
  "visibility": "household",
  "status": "active",
  "repos": [
    {"name": "runtime", "remote": "roughcoder/jarvis", "default": true},
    {"name": "cockpit", "remote": "roughcoder/jarvis-cockpit"},
    {"name": "wacli", "remote": "roughcoder/wacli"},
    {"name": "infra", "remote": "roughcoder/jarvis-infra"}
  ],
  "links": {"jira": "JARV", "urls": []},
  "files_root": "jarvis-workspace/projects/jarvis/files"
}
```

| Field | Meaning |
| --- | --- |
| `id` | Stable slug used in session ids and metadata. |
| `name` | Display name. |
| `peer_id` | Honcho peer id, always `project:<id>`. |
| `aliases` | Phrases Jarvis can resolve from speech. |
| `owner` | Authority: who can archive, share, or make private. |
| `members` | People allowed to see and update the project. |
| `visibility` | `household`, `private`, or `shared`. |
| `status` | `active`, `paused`, `archived`. |
| `repos` | Zero or more git repos, each with a short spoken `name`, a `remote`, and at most one `default: true`. Projects are multi-repo by nature (Jarvis itself has four). |
| `links` | Other external resources: issue trackers, URLs, docs. |
| `files_root` | Jarvis-owned original file storage location. |

Repos are a list, never a single field. Tools that act on a repo (worker
jobs, GitHub lookups) resolve it as: explicit repo named in the request →
project's `default` repo → ask ("which repo — runtime, cockpit, wacli or
infra?"). The short `name` is chosen to be speakable.

Home projects default to `household`; work projects will often be
`private`/`shared`. The registry lives under the private Jarvis workspace,
not the public repo.

### Cockpit integration

The Cockpit API (`jarvis api`, `src/jarvis/orchestration/api.py`) gains
read-only project routes in v1:

```
GET  /v1/projects                            registry list, membership-filtered
GET  /v1/projects/{id}                       full entry: repos, links, members, status, files
GET  /v1/projects/{id}/memory                cached representation + recent findings/decisions
GET  /v1/projects/{id}/threads               orchestrator threads for the project
POST /v1/projects/{id}/threads               open a new orchestrator thread
POST /v1/projects/{id}/threads/{tid}/turns   send a turn (reply streams over SSE)
```

Two boundary rules are locked in:

- **The Cockpit never talks to Honcho directly.** All reads go through the
  Jarvis API so the capability gate and the network boundary hold.
- **Listings are membership-filtered by the authenticated requester.** A
  private work project does not appear in a kid's Cockpit view.

Writes (create project, add finding) arrive later as POST routes; voice and
curation tools cover them initially.

### Voice interaction (must work hands-free)

Projects are driven by voice as much as by the Cockpit: "open project
jarvis", "move to the bird story project", "switch to Julia's study space",
"close the project".

- **Switching is a gated tool action.** The switch tool resolves the spoken
  phrase against registry `aliases` (fuzzy, STT-tolerant), checks membership,
  and sets the **active project** on the caller's (device × user) session
  context. Ambiguity or a failed match asks back rather than guessing.
- **Because the switch is an explicit tool invocation, it may refresh live**
  (memory-as-tool carve-out): on open, fetch/refresh the project peer's
  representation into the local cache while acknowledging ("opening the
  Jarvis project"). First-ever open of a project tolerates an empty cache.
- **Subsequent turns stay on the hot-path invariant**: while a project is
  active, the ambient context is the personal cache **plus the active
  project's cached representation** — both local file reads, no network.
- Active project is per (device × user) session state (`contexts.py`), not
  global: Neil on the office mac and Alice in the kitchen have independent
  active projects. It expires with the session context or on "close project".

### Orchestrator conversations (Cockpit)

A long-lived, project-bound chat thread with full project knowledge that can
spin up work and update memory. Supported natively by this model:

- **Thread = Honcho session** `project:<id>:orchestrator:<thread-id>`; a
  project can have many threads. Lane 1 applies: the human's messages are
  theirs, the orchestrator's are `jarvis`. Honcho's session persistence and
  summaries make long-lived threads cheap to resume.
- **Context is assembled live at turn start** — the cockpit is not the voice
  hot path, so it can afford: registry entry (repos, links, status) + project
  representation + recent explicit conclusions (decisions/findings verbatim,
  filtered by `project_id`) + thread history/summary.
- **Work**: the orchestrator is a `BrainSession` with the existing tool layer
  (worker jobs against the project's `repos`, background lane, Jira/GitHub
  MCP), running under the requester's capabilities, project-scoped. Job
  completions report back into the thread and via proactive push.
- **Memory updates go through Lane 2**: the orchestrator proposes recording
  findings/decisions ("log that as a decision?") and writes explicit
  conclusions on the project peer — attributed, gated, deletable. It never
  silently auto-curates shared project memory.

Turn flow:

```text
Cockpit UI -> POST /v1/projects/{id}/threads/{tid}/turns   (Jarvis API, never Honcho)
  -> membership check (capability gate)
  -> BrainSession turn: live project context + LLM + tools
  -> reply streamed over the existing SSE channel
  -> cold path: transcript -> Honcho session, refresh project cache
```

New build implied: thread routes on the Cockpit API and a cockpit connector
bridging threads to brain sessions (same shape as `connectors/text.py`, over
HTTP/SSE instead of a terminal).

### Jarvis as an MCP server

Jarvis is already an MCP *client* (brain → external tool servers). A second,
inverse lane exposes brain powers as an MCP *server* (`jarvis mcp-serve`,
streamable HTTP + stdio) so external agents — Claude Code in a project repo,
desktop assistants — can use the household brain:

| MCP tool | Backs onto |
| --- | --- |
| `project_list` / `project_get` | Registry, membership-filtered. |
| `memory_search` | Live dialectic/representation query (`search_query`, optional `target`), access-matrix gated. |
| `record_finding` / `record_decision` / `remember` | Lane 2 explicit conclusions. |
| `upload_file` | File vault + Honcho ingestion session (the spec-upload flow). |
| `open_thread` / `send_turn` | Orchestrator threads. |

Rules:

- The MCP server is a **boundary peer**: it translates tool calls into brain
  requests over the protocol/API and never touches Honcho or the registry
  directly. There remains exactly one copy of project knowledge.
- **Every MCP token maps to a principal** and inherits that principal's
  capabilities at the gate. No anonymous access; tokens are per-principal
  and revocable.
- **External-agent writes are attributed and reviewable**: conclusions carry
  `recorded_by`, `channel: mcp`, and an `agent` tag, so agent-written memory
  is auditable and deletable fact-by-fact.

## Sessions

Sessions are threads or import contexts — and, because observation scope
equals session participation, they are also the privacy boundary (see
Privacy). They group related messages; they are not long-term memory
identities.

| Pattern | Purpose |
| --- | --- |
| `voice:<person>:<device>` | Rolling voice conversation for a person on a device. |
| `whatsapp:<person>` | WhatsApp thread for a person. |
| `text:<person>` | Terminal/text connector thread. |
| `background:<person>:<job-id>` | Detached background work. |
| `project:<project-id>:inbox` | Unclassified project notes. |
| `project:<project-id>:uploads:<doc-id>` | Extracted chunks from one uploaded document. |
| `project:<project-id>:meetings:<date-or-id>` | Meeting transcript or notes. |
| `project:<project-id>:orchestrator:<thread-id>` | Long-lived Cockpit orchestrator thread for a project. |

(The draft's `findings`/`decisions`/`specs` sessions are gone: findings and
decisions are now conclusions, not messages. Uploads and meeting transcripts
remain message-based because they are genuine content ingestion, and one
session per document keeps retraction surgical — deleting the session removes
the document.)

Session lifecycle: rolling conversational sessions persist (Honcho summarises
them); `background:*` and `uploads:*` sessions accumulate and should be
prunable by policy (archive/delete after retention). Set session peer
membership **before** writing messages — there is no retroactive reasoning.

Known tradeoff: device-scoped voice sessions mean thread context does not
follow a person between rooms mid-conversation (the representation does).
Acceptable for now; revisit if it bites.

### WhatsApp

- A 1:1 thread is `whatsapp:<person>` with two peers (the principal +
  `jarvis`); sender-number → principal mapping is the identity layer's job.
  Because observation scope = session participation, 1:1 threads are
  structurally private.
- WhatsApp is async, so it qualifies as a live-ambient-context channel
  (fresh representation + conclusions at turn start, cache fallback), and it
  carries the full tool surface: curation, memory queries, project
  switching, background work — with the thread acting as the "device" for
  per-(device × user) active-project state.
- **Group threads**: a multi-peer session over every sender (set membership
  before writing). Senders map to principals or contacts by phone number;
  **an unmapped sender auto-creates a contact** (strong identity: stable
  number + push name) and their messages are written Lane 1 to that peer, so
  Jarvis learns about them passively. Speaking grants no rights — contact
  requesters are guest tier at the gate. If a contact later gets an account,
  the standard promotion path applies (identity-layer event, same peer).

## Messages and Conclusions

### Voice turn (Lane 1)

```json
{
  "session_id": "voice:neil:mac",
  "messages": [
    {
      "peer_id": "neil",
      "content": "I prefer concise answers in the morning.",
      "metadata": {"channel": "voice", "device_id": "mac"}
    },
    {
      "peer_id": "jarvis",
      "content": "Noted.",
      "metadata": {"channel": "voice", "device_id": "mac"}
    }
  ]
}
```

### Declared fact about a person (Lane 2)

```json
POST /v3/workspaces/jarvis-home/conclusions
{
  "peer_id": "contact:klaus",
  "level": "explicit",
  "content": "Klaus does not work on Fridays.",
  "metadata": {
    "recorded_by": "neil",
    "source": "spoken",
    "channel": "voice",
    "observed_at": "2026-07-04"
  }
}
```

### Project finding / decision (Lane 2)

```json
POST /v3/workspaces/jarvis-home/conclusions
{
  "peer_id": "project:jarvis",
  "level": "explicit",
  "content": "Decision: Jarvis uses self-hosted Honcho v3 as the memory backend; projects are modeled as entity peers.",
  "metadata": {
    "project_id": "jarvis",
    "artifact_type": "decision",
    "recorded_by": "neil",
    "status": "accepted"
  }
}
```

### Spec upload

Store the original file in the Jarvis file vault, then use Honcho's file
upload to ingest extracted text as messages under the project peer in a
dedicated session:

```json
{
  "session_id": "project:jarvis:uploads:memory-backend-spec",
  "peer_id": "project:jarvis",
  "metadata": {
    "project_id": "jarvis",
    "artifact_type": "spec",
    "title": "Memory Backend Spec",
    "uploaded_by": "neil",
    "source": "file",
    "content_hash": "sha256:...",
    "original_path": "jarvis-workspace/projects/jarvis/files/memory-backend-spec.pdf"
  }
}
```

Honcho keeps extracted text, not the original. Jarvis owns the file vault.

### Metadata envelope

| Key | Examples | Purpose |
| --- | --- | --- |
| `project_id` | `jarvis` | Project filtering and routing. |
| `artifact_type` | `spec`, `finding`, `decision`, `note`, `meeting` | Retrieval and summarisation. |
| `channel` | `voice`, `whatsapp`, `text`, `file`, `background` | Source tracking. |
| `device_id` | `mac`, `kitchen-pi` | Device-specific debugging/context. |
| `recorded_by` | `neil` | Attribution for declared facts. |
| `observed_at` | ISO date | When the fact was true/observed — REQUIRED on every explicit conclusion from day one, so retrieval can surface age and consolidation can revisit stale facts. |
| `source_url` | URL | Research/external provenance. |
| `content_hash` | `sha256:...` | Deduplication and migration. |
| `original_path` | path under `jarvis-workspace` | File vault linkage. |

Metadata is the source of truth for attribution and provenance. Do not encode
it in content prose ("Finding recorded by Neil: …") — prose prefixes drift and
teach the deriver boilerplate.

## Reading Memory

Two read modes with different rules:

**Memory-as-context (ambient): cache-only on the voice hot path.** The
in-turn read is a local file read of the speaking principal's cached
representation, plus the active project's cached representation when one is
open. Never a network call. The cache-only rule is a *voice-latency* rule:
interactive non-voice channels (Cockpit orchestrator threads, text/WhatsApp
where acceptable) may assemble ambient context live at turn start, since a
second of context assembly is invisible there. The network-boundary and
best-effort rules still apply — a dead Honcho degrades a cockpit turn to
cached context, it never breaks the turn.

**Memory-as-tool (explicit, mid-turn): live allowed.** When the user asks a
memory question — "what do we know about Klaus?", "what open questions are on
the study-space project?", "what did I tell you about the renovation?" — a
capability-gated tool makes a live representation/dialectic query
(`search_query`, optional `target=`). This is the same shape as `web_search`:
a network tool whose answer is the deliverable. The hot-path invariant is
about ambient context, not about tools the user explicitly invoked.

Cross-perspective queries (`target=`) are tool-only, never cached — the cache
matrix stays one blob per principal plus active projects.

## Privacy and Access

Three mechanisms, each doing one job:

1. **Sessions bound derivation (write side, structural).** Observation scope
   equals session participation, so nothing said in a private thread can
   enter anyone else's derived representation. Route by channel and
   participants.
2. **The capability gate bounds queries (read side, enforced).** A small
   explicit matrix: a requester may query (a) their own peer, (b) `target=`
   views they themselves own, (c) contact peers they own or that are shared
   with them, (d) project peers where they are a member, and (e) — the
   guardian rule — a guardian-tier principal may query a minor principal's
   peer ("what's been on Alice's mind?"). Guardianship is recorded in the
   identity layer's trust tiers and lapses when the child's tier is
   upgraded; it is a read right only. The Cockpit API applies the same
   matrix. Deny by default.

Minors are otherwise full principals: identical curation and query powers
(remember/forget, contacts, projects). Only device/tool gating differs by
trust tier, as it already does.
3. **The write-routing rule bounds shared peers (curation side, policy).**
   Personal facts go on personal peers; only genuinely shared facts go on
   shared entity peers. A shared peer's representation is readable by all its
   members and cannot be partially hidden.

`visibility` metadata is a label for filtering and audit. It enforces
nothing; the gate and the registry do.

## Correction and Forgetting

- **Forget a fact**: `query-conclusions` (semantic) → confirm with the user →
  `delete-conclusion`. Works for both declared and derived conclusions.
- **Correct a fact**: delete the wrong conclusion *and* write a new explicit
  one. Honcho reconciles contradictions and tracks them as a first-class
  conclusion level.
- **Retract a document/import**: delete its dedicated session and the
  conclusions filtered to it (messages themselves are not deletable —
  session-per-document exists precisely for this).
- **Facts age**: "Klaus is off Fridays" is true until it isn't. Honcho's
  reasoning reconciles contradictions when new information arrives, but
  nothing revisits stale declared facts on its own. Mitigations: every
  explicit conclusion carries `observed_at` (mandatory, see metadata) so
  retrieval and the answering prompt can weigh age ("as of last March, …"),
  and the periodic dream/consolidation pass is pointed at old declared
  facts as review candidates. Do not silently expire facts — surface age,
  let contradiction or the owner retire them.
- **Open risk to test at implementation**: whether the dreamer can re-derive
  a deleted conclusion from still-existing source messages. If so, deletions
  of *derived* facts need a guard (e.g. a correcting explicit conclusion
  written alongside the delete). Declared facts have no source messages and
  are safe.

## Hot/Cold Path

Unchanged invariant, with sequencing made explicit.

Hot path:

```text
wake -> capture -> STT -> read local cached memory -> LLM -> stream TTS
```

Cold path (after the reply, fire-and-forget):

```text
write turn messages to Honcho (Lane 1, best-effort)
flush the Lane 2 outbox (delivered-or-reported; the in-turn step was only
  the local journal append)
wait for deriver idle for the affected peers (bounded — refresh anyway after N seconds)
refresh local caches for the speaking principal and the active project
record timing/failure as best effort
```

The bounded deriver-idle wait fixes the record-then-ask staleness race
without letting a busy deriver starve the cache. Never add a live
representation/chat call to the ambient hot path; the memory tool is the only
sanctioned live read.

Refresh policy is a cost decision: per-peer dialectic refreshes are LLM
calls. Refresh the speaking principal on every cold path (debounced, as
today) and project peers only when they were written to or explicitly
switched to — not the whole registry on a timer. A project *switch* is an
explicit tool action and may refresh that project's cache live within the
switch turn; every later turn reads the cache.

## Cache Shape

One cache file per cached peer, keyed by a **sanitised** form of the peer id
(`:` and separators mapped, e.g. `project:jarvis` → `project-jarvis`). The
current `_cache_path` interpolates raw peer ids and must be fixed before
entity peers land.

```text
.cache/representation.json
.cache/representation-neil.json
.cache/representation-jules.json
.cache/representation-project-jarvis.json
```

Contacts are not routinely cached — contact reads go through the memory tool.

## Honcho Configuration

| Setting | Value | Why |
| --- | --- | --- |
| `observe_me` on `jarvis` peer | `false` | SOUL.md is the personality authority; don't pay the deriver to study Jarvis. |
| `observe_others` | off initially | Lane 1 already lands hearsay in the speaker's rep; enable per-session later if shared-session perspective segmentation is needed. |
| Deriver custom instructions on `project:*` peers | set | The deriver is tuned for conversation; tell it these peers hold artifacts (uploads, meeting notes). |
| Peer cards | seed for principals and contacts | Grounding: name, relationship, disambiguators. |
| Honcho LLM provider | OpenAI transport -> LiteLLM | Use LiteLLM route names with per-caller `base_url` overrides; deriver requires `structured_output_mode: json_object`. |

## v3-Only Runtime

- Production cut over to Honcho v3 on 2026-07-05.
- Rollback support for the old Honcho v2 stack was retired on 2026-07-19.
- `MemoryClient` now uses the v3 HTTP API unconditionally behind the existing
  brain memory interface.
- The profile-fact seeding command remains available for explicit v3 conclusion
  imports, and the conclusion provenance sidecar remains load-bearing because
  Honcho v3.0.11 does not round-trip conclusion metadata.

## Queries Jarvis Should Support

Ambient (cache): the standing "what you know about the user" context block.

Tool (live):

```text
What do we know about Klaus?
When is my sister's birthday?           (answered from the asker's own rep)
What is the current state of the Jarvis project?
What open questions are there in Julia's study space project?
What decisions have we made about Honcho?
```

Curation (write):

```text
Remember about Klaus: he's off Fridays.        (creates contact on first use)
Remember Julia's birthday is in March.
Open the Jarvis project.  /  Move to the bird story project.  /  Close the project.
Run the failing-tests job on the cockpit repo.   (repo resolution via the registry)
Add a finding to the Jarvis project: ...
Record a decision for Julia's study space: ...
Upload this as a spec for the Jarvis project.
Forget what I told you about ...
```

## Dependencies

### Upstream (this model depends on)

| Dependency | What we need from it | Risk / action |
| --- | --- | --- |
| Honcho v3 (self-hosted, `ghcr.io/plastic-labs/honcho:v3.x`) | Peers, sessions, conclusions CRUD, observer scopes, file uploads, queue status | Verified at v3.0.11; pin the image, re-verify on bumps. |
| LiteLLM gateway (OpenAI-compatible transport) | All Honcho reasoning (deriver/dialectic/dreamer/summary/embeddings) routes through it | Validated against v3.0.11; keep `structured_output_mode=json_object` for deriver calls. |
| Identity layer (`identity.py`, trust tiers) | Maps devices/channels/voices to principals; the requester for every gate check | Exists; contacts add no requirement. |
| Capability gate (`capabilities.py`) | Deny-by-default enforcement of the access matrix and curation writes | Exists; needs the memory/curation capability definitions. |
| Registry storage (private Jarvis workspace) | Projects and contacts: identity, aliases, owner/members, repos, links | New; single-writer via the brain, so a JSON/SQLite store suffices. |
| File vault (`files_root`) | Canonical storage for original uploads | New convention; plain filesystem. |
| Session contexts (`contexts.py`) | Per-(device × user) active-project state | Exists; add the active-project field. |

### Downstream (depends on this model)

| Consumer | What it consumes | Impact |
| --- | --- | --- |
| `BrainSession` / turn loop | Ambient context: personal + active-project caches (voice); live assembly (non-voice) | Prompt-assembly change only, behind the backend interface. |
| Voice tools | open/switch/close project, curation commands, the memory tool | New gated tools + alias resolution. |
| Worker tool / jobs | Project `repos` list for job targeting (explicit → default → ask) | Worker tool gains repo resolution from the registry. |
| Jira/GitHub/MCP tools | `links.jira`, `repos[].remote` for scoping | Read the registry, not hardcoded config. |
| Cockpit API + UI | `/v1/projects*` routes, orchestrator threads, SSE | New routes + cockpit connector; never touches Honcho directly. |
| MCP server lane (`jarvis mcp-serve`) | memory_search, curation, uploads, threads as MCP tools | New boundary peer; per-principal tokens; same gate. |
| Connectors (whatsapp/text) | Session naming (`whatsapp:<person>`, `text:<person>`), Lane 1 writes | Rename/confirm session ids; no behavioural change. |
| Heartbeat / proactive | May read caches; must not trigger live memory reads on idle | Confirm it stays cold-path. |
| `jarvis status` / traces | Memory backend reachability, cache freshness, deriver queue | Extend probes to v3 endpoints (`/health` exists in v3). |

### Build order

1. Validate LiteLLM OpenAI-transport routing against a v3 container.
2. Memory backend interface + v3 client (peers, sessions, messages,
   conclusions, representation, queue status), `jarvis-dev` workspace.
3. Registry (projects + contacts) with owner/members/visibility, repos,
   aliases.
4. Caches for arbitrary peers (sanitised keys) + cold-path sequencing.
5. Curation tools + memory tool + access matrix in the gate.
6. Voice project switching (active-project context state).
7. Cockpit routes (`/v1/projects*`) + orchestrator threads/connector.
8. MCP server lane (`jarvis mcp-serve`): per-principal tokens over the same
   gate and tools.
9. Cutover: fresh `jarvis-home` on v3; seed profile-file facts as explicit
   conclusions (verified by read-back). Completed 2026-07-05; v2 retired
   2026-07-19.

## Implementation Implications

1. A memory backend interface so `BrainSession` stays decoupled from Honcho's
   HTTP details.
2. A registry for entity peers (projects and contacts) under the private
   Jarvis workspace: identity, aliases, owner/members/visibility, repos,
   links.
3. Curation tools (Lane 2): remember/forget/correct for people and contacts;
   add-finding/record-decision/upload-spec for projects; all capability-gated.
4. The memory tool (live queries with `search_query`/`target`), gated by the
   access matrix.
5. Voice project switching: alias resolution, per-(device × user) active
   project, live refresh on switch, cached reads thereafter.
6. Multi-repo resolution in the worker/GitHub tools (explicit → default →
   ask), driven by the registry `repos` list.
7. Cockpit routes: `/v1/projects`, `/v1/projects/{id}`,
   `/v1/projects/{id}/memory`, orchestrator threads — membership-filtered,
   never touching Honcho directly — plus the cockpit connector bridging
   threads to brain sessions.
8. Cache support for arbitrary peers with sanitised cache keys; bounded
   deriver-idle refresh sequencing.
9. Tests: attribution routing, access-matrix enforcement, correction
   round-trip (declare → query → forget), project switch via alias, repo
   resolution, and the dreamer re-derivation probe.

## Non-Goals

- Do not create a Honcho workspace per project, per user, or per device.
- Do not subject-attribute messages; the curation lane exists for that.
- Do not auto-create contact peers from *spoken name mentions* (weak
  identity). Auto-creation is for channel senders with stable identifiers.
- Do not put Jarvis personality into Honcho memory. `SOUL.md` remains
  authoritative.
- Do not make hosted/remote Honcho availability part of the voice hot path.
- Do not store private original files only in Honcho. Jarvis owns the file
  vault.
- Do not let the Cockpit (or any UI) query Honcho directly.
