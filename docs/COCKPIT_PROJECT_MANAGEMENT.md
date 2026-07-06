# Cockpit Project Management

Date: 2026-07-05

This note specifies how projects are **viewed, edited, and managed** from the
Cockpit (and by external agents over MCP): create/edit the project entry, manage
repos and members, change visibility/status, add and retract findings/decisions,
and upload documents. It builds on the read-only project surface and orchestrator
threads that already shipped (`docs/HONCHO_MEMORY_MODEL.md`, "Cockpit
integration" and "Orchestrator conversations"). It records decisions taken in
design so the build is a matter of assembly, not re-litigation.

## What exists today (shipped)

Read + converse, membership-filtered by the authenticated caller:

| Surface | What |
| --- | --- |
| `GET /v1/projects` | Registry list, membership-filtered. |
| `GET /v1/projects/{id}` | Full entry: repos, links, members, status, `files_root`. 404 for non-members. |
| `GET /v1/projects/{id}/memory` | Cached representation + recent findings/decisions. |
| `GET`/`POST /v1/projects/{id}/threads` | List / open orchestrator threads; `GET` accepts `include_archived`. |
| `POST /v1/projects/{id}/threads/{tid}/turns` | Send a turn (reply streams over SSE). |
| `POST /v1/projects/{id}/threads/{tid}/archive` / `/unarchive` | Hide or restore a project thread. |
| MCP tools | `project_list`, `project_get`, project write tools, `memory_search`, `record_finding`, `record_decision`, `remember`, `forget`, `correct`, `open_thread`, `send_turn`, `archive_thread`, `unarchive_thread`, `upload_file`. |

This feature now also includes **registry-entry writes** (create/edit the entry,
repos, members, visibility, status) and **file upload** through the same
brain-owned write path described below.

## Locked decisions

1. **Registry writes route through the brain.** The brain is the *sole* registry
   writer (the registry store assumes a single writer). The Cockpit API and the
   MCP server **forward** write requests to the brain over the protocol; reads
   stay as direct per-request registry reads (the brain's atomic writes keep
   those consistent). This is the load-bearing constraint — never let the API
   process write the registry file directly.
2. **One gated operation set, two front doors.** REST routes (`/v1/projects*`)
   serve the Cockpit UI; matching MCP tools serve external agents. Both call the
   same brain-owned operations and the same access matrix, so they cannot drift.
3. **Gate by role.**
   - **Member** may edit *content*: name, aliases, status (active↔paused),
     links, `files_root`, repos (add/remove/reorder/default), findings and
     decisions (add + forget/correct), and file uploads.
   - **Owner** only: visibility, member management, archive, delete.
4. **Entry edits are transactional; memory writes are per-action.** The entry is
   small structured config — edit a change-set and Save (atomic `PATCH`), so a
   grouped change like *visibility→shared + add members* applies as a unit and
   is validated as a whole. Findings/decisions/uploads are discrete commands,
   fired per action, carrying attribution and `observed_at`.
5. **Owner-only actions are separate, explicit endpoints** (not one big PATCH
   with per-field gating). This maps 1:1 to the gate, makes the owner surface
   obvious in the UI (disabled unless owner), and avoids "the patch contains one
   forbidden field — reject the whole thing?" ambiguity.
6. **No optimistic concurrency for now.** Single household, low edit frequency,
   single brain writer → last-write-wins is acceptable. A version/etag check can
   be added later without reshaping the API.
7. **Three edit modalities, one operation.** The structured form (Save → PATCH),
   the MCP tools, and the orchestrator thread ("make this shared and add the
   infra repo") all funnel through the same gated brain-owned write. The thread
   can do anything the form can, subject to the same gate.

## Architecture

```text
Cockpit UI ──REST──▶ jarvis api ──protocol──▶ brain (sole registry writer)
External agent ──MCP tool──▶ mcp-serve ──protocol──▶ brain ─────────────────┘
                                                        │
                                    registry (JSON, single-writer, atomic)
                                    memory backend (Honcho v3) for Lane 2 + uploads
```

- A new **registry-write protocol message** carries the operation from the API /
  MCP boundary peer to the brain. One message type with an `op` discriminator
  (`project.create`, `project.update`, `project.repos.set`,
  `project.members.set`, `project.visibility.set`, `project.archive`,
  `project.delete`) plus the payload and the authenticated requester. The brain
  validates against the gate, applies through `RegistryStore`, and returns the
  updated entry (or a structured error).
- The API and MCP layers hold **no registry-write logic** — they authenticate
  the caller, translate to the protocol message, and relay the result. Reads are
  unchanged (the API reads the registry per request).
- Memory writes (findings/decisions/forget/correct) reuse the existing Lane 2
  curation path; the boundary peers never touch Honcho directly.

## Access matrix (writes)

Enforced in the shared gate (`capabilities.py`), the same place the read matrix
lives, so REST, MCP, and the thread inherit it. Deny by default.

The Cockpit can query the effective projection for the current caller with
`GET /v1/projects/{id}/permissions`. Its booleans mirror the shared
`can_edit_project` member gate and `can_admin_project` owner gate used by the
brain in `project_management.py`; it is a read-only projection, not a separate
policy source. A visible non-member is reported as `role: "viewer"` with all
booleans false; projects outside the caller's visibility set still return 404.
`can_archive_thread` mirrors the member gate on the thread archive routes.

| Operation | Who |
| --- | --- |
| Create project | Any principal (becomes `owner`). |
| Edit name / aliases / status(active↔paused) / links / `files_root` | Member. |
| Repos: add / remove / reorder / set default | Member. |
| Add finding / decision; forget / correct a memory | Member. |
| Upload a file to the project | Member. |
| Archive / unarchive thread | Member. |
| Change visibility | Owner. |
| Add / remove members | Owner. |
| Archive / unarchive project | Owner. |
| Delete project | Owner. |

External MCP agents run under their principal's capabilities, so an agent acting
as a member can edit content and repos but cannot change visibility, manage
members, or delete — for free, via the shared matrix.

## REST API

Entry (transactional; owner-only actions separate):

```
POST   /v1/projects                     create; creator becomes owner
PATCH  /v1/projects/{id}                member-editable fields, applied atomically
PATCH  /v1/projects/{id}/visibility     owner: {visibility: household|private|shared}
POST   /v1/projects/{id}/members        owner: add member(s)
DELETE /v1/projects/{id}/members/{who}  owner: remove a member
POST   /v1/projects/{id}/archive        owner: status -> archived (reversible)
POST   /v1/projects/{id}/unarchive      owner: status -> active
DELETE /v1/projects/{id}                owner: delete the entry
```

- `PATCH /v1/projects/{id}` accepts only member-editable fields (name, aliases,
  status active↔paused, links, `files_root`, repos). A body containing an
  owner-only field is rejected (those have their own routes). Repos are validated
  as a set (at-most-one default; unique names) on the whole entry.
- All of the above 404 for non-members (indistinguishable from missing), same as
  the read routes; owner-only routes 403 for a non-owner member.

Memory (per-action, Lane 2, attributed):

```
POST   /v1/projects/{id}/findings       member: {content, ...}
POST   /v1/projects/{id}/decisions      member: {content, status?, ...}
POST   /v1/projects/{id}/memory/forget  member: retract (query -> confirm -> delete)
POST   /v1/projects/{id}/memory/correct member: delete + replacement
```

Each conclusion carries the envelope: `project_id`, `artifact_type`
(`finding`/`decision`), `recorded_by`, `observed_at`, `content_hash`. Retraction
of a *derived* project fact follows the Jarvis-side suppression model (see the
memory-model doc); declared conclusions delete cleanly.

## MCP tools

Mirror the REST operations for external agents, forwarding to the same brain op:

| Tool | Backs onto |
| --- | --- |
| `project_create` / `project_update` | Registry entry write. |
| `project_set_repos` | Registry repos write. |
| `project_set_visibility` / `project_set_members` / `project_archive` / `project_delete` | Owner-gated registry writes. |
| `record_finding` / `record_decision` / `forget` / `correct` | Lane 2 (record/* exist; add forget/correct). |
| `project_list_files` / `upload_file` / `retract_file` | File manifest + vault + Honcho ingestion (see below). |
| `open_thread` / `send_turn` / `archive_thread` / `unarchive_thread` | Orchestrator thread lifecycle and turns. |

MCP writes carry `recorded_by`, `channel: mcp`, and an `agent` tag, as
established for the MCP server lane.

## File upload

Uploading a document (e.g. a spec) is **project content**: **member-gated,
per-action, routed through the brain** (which owns the file vault and is the
single writer).

Flow:

1. **Vault first (durable).** The original file is written to the Jarvis-owned
   file vault under the project's `files_root` — local, durable, backed up.
   Jarvis owns the original; it is the source of truth.
2. **Then Honcho ingestion (queued).** Honcho v3's file-upload endpoint extracts
   text, chunks it, and creates messages attributed to the `project:<id>` peer in
   a **dedicated session** `project:<id>:uploads:<doc-id>`. The extracted text
   becomes derivable project memory. Honcho keeps only extracted text; the
   original stays in the vault. Delivery follows the delivered-or-reported
   pattern (vault write is synchronous and durable; ingestion is queued).
3. **Provenance:** `artifact_type: spec` (or `note`/`meeting`), `title`,
   `uploaded_by`, `source: file`, `content_hash`, `original_path` under the vault.
4. **Manifest:** the brain records each upload in a durable project file
   manifest (`doc_id`, `title`, `session_id`, `original_path`, `content_hash`,
   `artifact_type`, `uploaded_by`, `observed_at`) so Cockpit and MCP can list
   project documents.
5. **Retraction is surgical:** each document gets its own session, so removing a
   document = delete that one upload session (messages are not individually
   deletable in Honcho; session-per-document exists precisely for this). The
   vault original is retained for audit/recovery; the manifest entry is marked
   retracted and hidden from default file lists.

Transport wrinkle (decide at build): **REST uses true multipart**
(`POST /v1/projects/{id}/files`) — the natural fit for Cockpit drag-and-drop.
**MCP file transport is clunky** (tool args are JSON), so the MCP `upload_file`
tool should take either a **text/markdown blob** (fine for the common
agent-uploading-a-spec case) or a **path/URL reference** the brain fetches —
*not* base64 binary in a tool argument. The two surfaces need not upload
identically.

## Build notes

- Reuse: `RegistryStore` write methods (mostly present), the shared access matrix
  and requester derivation (OAuth `sub` → `users/` principal → gate), the Lane 2
  curation path, and the cockpit connector pattern. New: the registry-write
  protocol message + brain handler, the REST write routes, the MCP write tools,
  the write capabilities in the gate, and the file vault + manifest + Honcho
  ingestion.
- Boundary rules hold: the Cockpit/MCP never touch Honcho or the registry storage
  directly; everything over the network boundary from env config; the brain is
  the single registry writer.
- Suggested build order: (1) write capabilities + access matrix; (2) brain-owned
  registry-write op over the protocol; (3) REST write routes; (4) MCP write
  tools; (5) memory forget/correct routes+tools; (6) file vault + `upload_file`
  (its own step). Voice/thread editing rides the same brain op for free.

## Non-goals

- No optimistic concurrency / multi-editor conflict resolution (single writer,
  last-write-wins) for now.
- The API/MCP never write the registry file directly — always through the brain.
- No binary-in-tool-arg uploads over MCP.
- Do not conflate registry-entry edits (structured CRUD) with project memory
  writes (Lane 2 Honcho conclusions) — same UI, two backends, kept separate.
