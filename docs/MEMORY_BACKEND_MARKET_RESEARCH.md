# Memory Backend Market Research

Date: 2026-06-30

This note compares hosted and self-hostable agent-memory systems as candidates
for Jarvis before moving from local Honcho to a hosted memory provider.

Decision update, 2026-07-04: Jarvis is sticking with Honcho. This document is
now historical market due diligence and fallback context. The active design is
`docs/HONCHO_MEMORY_MODEL.md`.

## Jarvis Requirements

Jarvis is a local-first voice assistant with a strict hot/cold path split.
Any memory backend must preserve these constraints:

- Voice hot path never blocks on a memory write or live reasoning call.
- Runtime talks to memory over a network boundary from configuration.
- User memory, project memory, and Jarvis personality remain separate.
- Memory can model people, the assistant, projects, documents, findings, and
  device/channel metadata.
- Hosted operation is available, with a credible escape hatch through export,
  self-hosting, or a clean adapter migration.
- Vendor outage must degrade to local cached memory rather than break a turn.

## Current Jarvis Integration Shape

Jarvis is already close to a backend-adapter architecture:

- `MemoryConfig` owns `MEMORY_*` settings, workspace id, user peer id,
  assistant peer id, cache path, and refresh interval.
- `MemoryClient.read_cached_representation()` is the hot-path read.
- `MemoryClient.write_turn()` and `MemoryClient.refresh_cache()` run on the
  cold path after the response.
- The current implementation is hardcoded to Honcho `/v2` REST endpoints, while
  Honcho hosted documentation is v3.

The correct migration shape is therefore a `MemoryBackend` interface with
vendor-specific adapters, not a rewrite of the brain/session layer.

## Decision Criteria

| Criterion | Weight | Notes |
| --- | ---: | --- |
| Hot/cold path compatibility | High | Async writes, bounded refresh, local cache possible. |
| Entity model | High | Must support users, assistant, and `project:*` entities cleanly. |
| Project/document support | High | Specs, findings, decisions, uploads, citations. |
| Hosted maturity | High | Reliable enough for household memory. |
| Export/delete/portability | High | Personal memory cannot be trapped. |
| Privacy/security | High | Household and user profile data is sensitive. |
| Self-host fallback | Medium | Useful if hosted terms, cost, or reliability changes. |
| Integration complexity | Medium | Python-first and REST-friendly preferred. |
| Cost predictability | Medium | Voice assistant writes continuously over time. |

## Shortlist

| Rank | Candidate | Fit |
| ---: | --- | --- |
| 1 | Honcho Hosted v3 | Best conceptual fit for Jarvis's peer/session/project model. |
| 2 | Zep Cloud | Strongest enterprise/temporal-graph alternative. |
| 3 | Mem0 Platform | Best simple hosted memory API fallback. |
| 4 | Cognee | Strong candidate for project/document graph memory, less direct for personal voice memory. |
| 5 | Supermemory | Strong context/RAG API, less natural as Jarvis's primary memory ontology. |
| 6 | LangMem/LangGraph | Good if we own the memory implementation. More engineering burden. |
| 7 | Letta | Powerful stateful-agent runtime, but too invasive for a memory-only backend swap. |
| 8 | OpenAI File Search/vector stores | Useful substrate for document search, not a complete memory backend. |

## Honcho Hosted v3

Sources:

- https://honcho.dev/docs/v3/documentation/introduction/quickstart
- https://honcho.dev/docs/v3/documentation/core-concepts/architecture
- https://honcho.dev/docs/v3/documentation/core-concepts/representation
- https://honcho.dev/docs/v3/documentation/features/storing-data
- https://honcho.dev/docs/v3/documentation/features/get-context
- https://honcho.dev/docs/v3/documentation/features/chat
- https://honcho.dev/docs/v3/documentation/features/advanced/file-uploads
- https://honcho.dev/docs/v3/documentation/features/advanced/using-filters
- https://honcho.dev/docs/v3/contributing/self-hosting
- https://github.com/plastic-labs/honcho

Honcho is the cleanest ontology match. Its v3 model is workspace, peer, session,
and message. Workspaces isolate environments or tenants. Peers are long-lived
entities whose representations are built over time. Sessions are interaction
threads or import contexts. Messages can contain normal conversations or
documents/actions with metadata.

For Jarvis, this maps directly:

- Workspace: `jarvis-home`, `jarvis-dev`, `jarvis-staging`.
- Peers: `neil`, `julia`, `jarvis`, `project:jarvis`,
  `project:julias-study-space`.
- Sessions: voice threads, WhatsApp threads, background jobs, project specs,
  project findings, uploaded document sessions.
- Messages: voice turns, extracted document chunks, findings, decisions,
  status updates.

Strengths:

- Best fit for project-as-peer.
- Reasoned peer representations, peer cards, summaries, and `peer.chat()`.
- File upload converts PDF/text/JSON into messages.
- Metadata/filtering supports project and channel identity.
- Async reasoning aligns with Jarvis cold-path model.
- Self-hosting exists.

Risks:

- Jarvis currently uses Honcho `/v2`; hosted docs and SDK are v3.
- Smaller open-source footprint than Mem0/Zep ecosystem projects.
- AGPL license matters for self-host/customization posture.
- Hosted pricing and enterprise controls need direct validation before commit.

Verdict: preferred POC baseline.

## Zep Cloud

Sources:

- https://help.getzep.com/concepts
- https://help.getzep.com/adding-messages
- https://help.getzep.com/adding-business-data
- https://help.getzep.com/sdk-reference/thread/get-user-context
- https://help.getzep.com/security-compliance
- https://www.getzep.com/pricing/
- https://github.com/getzep/graphiti

Zep is the strongest serious alternative. Its core model is a temporal knowledge
graph with users, threads, facts, observations, context blocks, and business
data. It has stronger enterprise positioning than Honcho: security docs mention
SOC 2 Type II, HIPAA BAA, RBAC, audit/API logs, BYOK, and BYOC.

Strengths:

- Mature temporal fact model with invalidation/provenance semantics.
- Thread/user context is designed for prompt injection.
- Business data can represent documents and project information.
- Strong governance/security posture.
- Graphiti open source gives an escape hatch for graph-memory concepts.

Risks:

- Less symmetrical entity model. Projects may need to be represented as users,
  business data, or separate graph namespaces.
- Potentially more enterprise-heavy than a household assistant needs.
- Could increase adapter complexity compared with Honcho or Mem0.

Verdict: best enterprise-grade alternative to test.

## Mem0 Platform

Sources:

- https://docs.mem0.ai/core-concepts/memory-types
- https://docs.mem0.ai/core-concepts/memory-operations/add
- https://docs.mem0.ai/core-concepts/memory-operations/search
- https://docs.mem0.ai/platform/features/v2-memory-filters
- https://docs.mem0.ai/platform/platform-vs-oss
- https://mem0.ai/pricing
- https://github.com/mem0ai/mem0

Mem0 is the strongest simple hosted memory API alternative. It supports layered
conversation/session/user/org memory, add/search/update/delete flows, metadata
filters, Platform and Open Source editions, and a large Apache-2.0 open-source
project.

Strengths:

- Simple API shape; likely easiest non-Honcho adapter.
- Strong open-source adoption and permissive license.
- Platform docs explicitly compare managed hosting with self-hosted OSS.
- Platform has managed hosting, dashboards, analytics, webhooks, custom
  categories, and memory export according to docs.

Risks:

- More memory-record/search oriented than representation/entity oriented.
- Jarvis would own more of the project/person ontology.
- Docs warn that retrievable memory should not contain unredacted secrets or PII
  without protection.

Verdict: best lightweight fallback.

## Cognee

Sources:

- https://docs.cognee.ai/core-concepts/overview
- https://docs.cognee.ai/core-concepts/main-operations/remember
- https://docs.cognee.ai/core-concepts/main-operations/recall
- https://docs.cognee.ai/core-concepts/main-operations/forget
- https://github.com/topoteretes/cognee

Cognee is a strong project/document knowledge candidate. It focuses on graph,
vector, and relational memory with operations like Remember, Recall, Improve,
and Forget. The docs emphasize graph-backed memory, session memory, datasets,
tags/node sets, loaders, chunkers, and ontologies.

Strengths:

- Good fit for project specs, findings, and structured document knowledge.
- Graph/vector architecture is useful for project spaces.
- Apache-2.0 and strong open-source adoption.

Risks:

- Less directly tailored to personal voice-assistant memory.
- May be better as an auxiliary project/document layer than the primary memory
  backend.

Verdict: consider for project knowledge if Honcho/Zep/Mem0 are weak on docs.

## Supermemory

Sources:

- https://supermemory.ai/docs/concepts/how-it-works
- https://supermemory.ai/docs/search
- https://supermemory.ai/pricing/
- https://github.com/supermemoryai/supermemory

Supermemory is a context/RAG-oriented memory API with document processing,
memory extraction, graph relationships, updates, extensions, derived memories,
and a processing pipeline from queued to searchable.

Strengths:

- Strong document/context API positioning.
- Graph-style update/extends/derives model is useful for evolving facts.
- MIT open-source project with strong adoption.

Risks:

- Less obviously suited to Jarvis's desired workspace/peer/session ontology.
- More likely to be a document/context layer than the personal memory source of
  truth.

Verdict: useful reference or secondary candidate, not primary POC.

## Letta

Sources:

- https://docs.letta.com/guides/core-concepts/stateful-agents
- https://docs.letta.com/guides/agents/memory
- https://github.com/letta-ai/letta

Letta is a stateful-agent platform. It has memory blocks, archival/recall
memory, tools, and agent state. It is technically relevant, but it overlaps with
Jarvis's brain/session/runtime responsibilities.

Verdict: not a memory-backend swap unless Jarvis intentionally becomes a Letta
agent runtime.

## LangMem / LangGraph

Sources:

- https://langchain-ai.github.io/langmem/
- https://docs.langchain.com/oss/python/concepts/memory

LangMem/LangGraph are good if Jarvis wants to own the memory system. Their
semantic/episodic/procedural distinction and hot-path/background-memory guidance
fit Jarvis conceptually.

Verdict: best build-your-own route, but not the fastest hosted migration.

## OpenAI File Search / Vector Stores

Sources:

- https://platform.openai.com/docs/guides/tools-file-search

OpenAI File Search and vector stores can support project document retrieval, but
they do not provide the higher-level memory ontology, async reasoning, peer
representations, or project/user separation that Jarvis needs.

Verdict: useful substrate, not a primary memory system.

## Project Modeling Comparison

| Backend | Natural project model | Notes |
| --- | --- | --- |
| Honcho | `project:*` peer | Cleanest. Project gets its own representation. |
| Zep | Business data / graph namespace / possibly project user | Strong graph, less direct entity symmetry. |
| Mem0 | `org_id`/metadata/custom categories | Works, but Jarvis owns the project ontology. |
| Cognee | Dataset/node set/graph memory | Good for project docs and findings. |
| Supermemory | container/metadata/document graph | Good for project context and search. |
| LangMem | custom store namespace | Full control, full burden. |

## Fallback Validation

Honcho is selected. If hosted Honcho later fails on pricing, security, export,
reliability, or project-memory behavior, validate fallbacks in this order:

1. Honcho Hosted v3
2. Zep Cloud
3. Mem0 Platform

Keep Cognee as a possible project/document layer if Honcho is weak on uploaded
specs and findings.

## Validation Tests

Honcho and any fallback candidate must pass the same tests:

1. Write a voice turn without delaying the assistant response.
2. Refresh local cached memory on the cold path.
3. Represent `neil`, `julia`, `jarvis`, `project:jarvis`, and
   `project:julias-study-space`.
4. Upload a project spec and recall it later by project.
5. Add a project finding and retrieve it by project.
6. Keep personal memory separate from project memory.
7. Search/filter by `project_id`, `channel`, `device_id`, `artifact_type`, and
   visibility.
8. Degrade cleanly when the vendor is down or slow.
9. Export/delete a user and a project.
10. Measure write latency, cold-refresh latency, context quality, and
    correctness of stale/updated facts.

## Current Recommendation

Use Honcho Hosted v3 as the selected backend because it matches Jarvis's
ontology best. Keep Zep and Mem0 as fallback references only.

Implementation should follow `docs/HONCHO_MEMORY_MODEL.md`: one workspace per
deployment/environment, people and projects as peers, sessions as
thread/import scopes, and messages attributed to the entity Honcho should learn
about.
