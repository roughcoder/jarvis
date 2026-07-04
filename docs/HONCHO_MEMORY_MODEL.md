# Honcho Memory Model for Jarvis

Date: 2026-07-04

This note records how Jarvis should use Honcho now that Honcho remains the
chosen memory backend. It turns the design discussion into a durable model for
future implementation.

## Source Evidence

Honcho v3 docs define these primitives:

- Workspaces are top-level containers and provide isolation between
  applications, environments, tenants, or product lines.
- Peers are the central representation target. A peer can be a user, agent, or
  any persistent entity.
- Sessions are interaction threads or import contexts. They contain messages
  and can include many peers.
- Messages are attributed to a peer, ordered in a session, support metadata, and
  trigger background reasoning.
- File uploads extract text, split it into message-sized chunks, and create
  messages attributed to the supplied peer.
- Representations are generated from reasoning over a peer's messages across
  sessions.

Primary docs:

- https://honcho.dev/docs/v3/documentation/core-concepts/architecture
- https://honcho.dev/docs/v3/documentation/core-concepts/representation
- https://honcho.dev/docs/v3/documentation/features/storing-data
- https://honcho.dev/docs/v3/documentation/features/advanced/file-uploads
- https://honcho.dev/docs/v3/documentation/features/advanced/queue-status

## Jarvis Vocabulary

| Jarvis concept | Honcho mapping | Rule |
| --- | --- | --- |
| Deployment/environment | Workspace | Hard isolation only. |
| Person | Peer | One peer per known human. |
| Jarvis assistant | Peer | One assistant peer, normally not personality authority. |
| Project | Jarvis-owned object backed by a peer | Project memory belongs to `project:*` peers. |
| Conversation/thread/import | Session | Scope local history, uploads, findings, and channel threads. |
| Turn/note/spec/finding/decision | Message | Attribute to the entity Honcho should learn about. |
| Long-term memory | Peer representation | Read via cold-path refresh/cache, not live on voice hot path. |

## Workspaces

Use workspaces only for hard isolation. Do not create one workspace per user,
project, room, channel, or device.

Recommended workspace ids:

| Workspace | Purpose |
| --- | --- |
| `jarvis-home` | Real household memory. |
| `jarvis-dev` | Local development and throwaway testing. |
| `jarvis-staging` | Migration and integration-test rehearsal. |
| `jarvis-demo` | Synthetic demo data. |

`MEMORY_WORKSPACE_ID` should select one of these. The production household should
usually be `jarvis-home`.

## Peers

Peers are the durable entities Honcho reasons about. For Jarvis, peers should
include humans, the assistant, and projects.

Examples:

```text
neil
julia
jarvis
project:jarvis
project:julias-study-space
project:house-renovation
```

Human peers represent people. Project peers represent bodies of work. The
assistant peer represents Jarvis as an actor in transcripts, but Jarvis
personality remains controlled by `SOUL.md`, not Honcho memory.

### Attribution Rule

Attribute a message to the entity that should learn from it:

- If the fact is about Neil, write it as `peer_id = neil`.
- If the fact is about Julia, write it as `peer_id = julia`.
- If the fact is about the Jarvis project, write it as
  `peer_id = project:jarvis`.
- If Jarvis said something useful only as transcript context, write it as
  `peer_id = jarvis`.

This is the most important rule. Wrong attribution pollutes the wrong
representation.

## Projects

Project is a Jarvis-owned concept. Honcho does not need a native Project
primitive because a project is naturally a persistent peer plus scoped sessions.

Jarvis should maintain a small local project registry:

```json
{
  "id": "jarvis",
  "name": "Jarvis",
  "peer_id": "project:jarvis",
  "aliases": ["the jarvis project", "jarvis"],
  "visibility": "household",
  "members": ["neil"],
  "status": "active"
}
```

The registry owns identity, aliases, file locations, permissions, and UI/routing
state. Honcho owns the reasoned project memory attached to the project peer.

Recommended project fields:

| Field | Meaning |
| --- | --- |
| `id` | Stable slug used in session ids and metadata. |
| `name` | Display name. |
| `peer_id` | Honcho peer id, always `project:<id>`. |
| `aliases` | Phrases Jarvis can resolve from speech. |
| `visibility` | `household`, `private`, or `shared`. |
| `members` | People allowed to see or update the project. |
| `status` | `active`, `paused`, `archived`. |
| `files_root` | Jarvis-owned original file storage location. |

Default visibility should be `household` for home projects unless explicitly
marked private.

## Sessions

Sessions are threads or import contexts. They should be stable enough to group
related messages, but not treated as long-term memory identities.

Recommended session id patterns:

| Pattern | Purpose |
| --- | --- |
| `voice:<person>:<device>` | Rolling voice conversation for a person on a device. |
| `whatsapp:<person>` | WhatsApp thread for a person. |
| `text:<person>` | Terminal/text connector thread. |
| `background:<person>:<job-id>` | Detached background work. |
| `project:<project-id>:inbox` | Unclassified project notes. |
| `project:<project-id>:findings` | Findings and research conclusions. |
| `project:<project-id>:decisions` | Accepted decisions and rationale. |
| `project:<project-id>:specs` | Project specs not tied to one uploaded file. |
| `project:<project-id>:uploads:<doc-id>` | Extracted chunks from one uploaded document. |
| `project:<project-id>:meetings:<date-or-id>` | Meeting transcript or notes. |

Use metadata as well as session naming. Session names make routing obvious;
metadata enables filtering and migration.

## Messages

Messages are the units written to Honcho.

### Voice Turn

```json
{
  "session_id": "voice:neil:mac",
  "messages": [
    {
      "peer_id": "neil",
      "content": "I prefer concise answers in the morning.",
      "metadata": {
        "channel": "voice",
        "device_id": "mac",
        "artifact_type": "turn"
      }
    },
    {
      "peer_id": "jarvis",
      "content": "Noted.",
      "metadata": {
        "channel": "voice",
        "device_id": "mac",
        "artifact_type": "turn"
      }
    }
  ]
}
```

### Project Finding

```json
{
  "session_id": "project:jarvis:findings",
  "messages": [
    {
      "peer_id": "project:jarvis",
      "content": "Finding recorded by Neil: Honcho projects should be modeled as peers so each project gets its own representation.",
      "metadata": {
        "project_id": "jarvis",
        "artifact_type": "finding",
        "recorded_by": "neil",
        "confidence": "high",
        "source": "spoken"
      }
    }
  ]
}
```

### Project Decision

```json
{
  "session_id": "project:jarvis:decisions",
  "messages": [
    {
      "peer_id": "project:jarvis",
      "content": "Decision recorded by Neil: Jarvis will use Honcho hosted as the memory backend and model projects as project peers.",
      "metadata": {
        "project_id": "jarvis",
        "artifact_type": "decision",
        "decided_by": "neil",
        "status": "accepted"
      }
    }
  ]
}
```

### Spec Upload

Jarvis should store the original file in Jarvis-owned storage, then upload or
write extracted content into Honcho under the project peer.

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

Honcho v3 file upload preserves extracted text as messages. It does not store
the original file as the canonical file vault. Jarvis should keep the original.

## Metadata

Use a consistent metadata envelope on project and conversation messages:

| Key | Examples | Purpose |
| --- | --- | --- |
| `project_id` | `jarvis` | Project filtering and routing. |
| `artifact_type` | `turn`, `spec`, `finding`, `decision`, `note`, `meeting` | Retrieval and summarization. |
| `channel` | `voice`, `whatsapp`, `text`, `file`, `background` | Source tracking. |
| `device_id` | `mac`, `kitchen-pi` | Device-specific debugging/context. |
| `recorded_by` | `neil` | Attribution for project facts. |
| `visibility` | `household`, `private`, `shared` | Access control. |
| `source_url` | URL | Research provenance. |
| `content_hash` | `sha256:...` | Deduplication and migration. |
| `original_path` | path under `jarvis-workspace` | File vault linkage. |

## Hot/Cold Path

This model must preserve the existing Jarvis memory invariant.

Hot path:

```text
wake -> capture -> STT -> read local cached memory -> LLM -> stream TTS
```

Cold path:

```text
write messages to Honcho
let Honcho reason asynchronously
refresh local cache for next turn
record timing/failure as best effort
```

Never add a live Honcho representation/chat/context call to the user-facing
voice hot path.

## Cache Shape

Current Jarvis caches one representation file per principal. With projects, the
same idea should extend to project peers.

Examples:

```text
.cache/representation.json
.cache/representation-neil.json
.cache/representation-julia.json
.cache/representation-project-jarvis.json
.cache/representation-project-julias-study-space.json
```

The cache key should be derived safely from the Honcho peer id. Do not use raw
peer ids as filesystem paths without sanitizing `:` and other separators.

## Queries Jarvis Should Support

Personal memory:

```text
What do we know about Neil's communication preferences?
What did Julia ask me to remember?
```

Project memory:

```text
What is the current state of the Jarvis project?
What open questions are there in Julia's study space project?
Summarize the findings for the Jarvis project.
What decisions have we made about Honcho?
```

Project write commands:

```text
Switch to the Jarvis project.
Upload this as a spec for the Jarvis project.
Add a finding to the Jarvis project: ...
Record a decision for Julia's study space: ...
```

## Implementation Implications

The eventual implementation should introduce these boundaries:

1. A memory backend interface so Honcho v2/local and Honcho v3/hosted can be
   swapped without changing `BrainSession`.
2. A project registry under the private Jarvis workspace, not the public repo.
3. Project-aware tools for switching active project, adding findings, recording
   decisions, and uploading specs.
4. Cache support for arbitrary peers, including `project:*` peers.
5. Metadata-based retrieval/filtering tests for project artifacts.

## Non-Goals

- Do not create a Honcho workspace per project.
- Do not merge project memory into a human peer's personal memory.
- Do not put Jarvis personality into Honcho memory. `SOUL.md` remains
  authoritative.
- Do not make hosted Honcho availability part of the voice hot path.
- Do not store private original files only in Honcho. Jarvis owns the file vault.
