# Modularity Review

**Scope**: PR #116 ("run review parents as code agents") — the code-agent orchestrator lane spanning `connectors/cockpit.py`, `orchestration/api.py`, `orchestration/orchestrator_grants.py`, `worker/server.py`, `worker/orchestrator_runtime.py`, `worker/orchestrator_mcp.py`, `worker/authority.py`, and the Codex/Claude provider adapters.
**Date**: 2026-07-11

## Executive Summary

Jarvis is an all-local voice assistant whose Cockpit tier dispatches coding work to provider sessions (Codex, Claude) on worker daemons across an HTTP boundary. PR #116 adds a new core capability: review-parent threads run as real code-agent sessions, reaching back into Jarvis through a signed, thread-scoped MCP tool surface. The overall [modularity](https://coupling.dev/posts/core-concepts/modularity/) of the change is healthy — the brain/worker boundary is respected everywhere, secrets stay request-scoped, and the new grant module is a small, cohesive [contract](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/). The most important finding is that the orchestrator *tool contract itself* — the set of four tool names and their argument shapes — has no single home: it is independently declared in four modules on both sides of the network boundary, in the most [volatile](https://coupling.dev/posts/dimensions-of-coupling/volatility/) part of the system. Second, retry semantics in the parent-continuation path depend on matching worker error-message *strings* across the HTTP boundary — implicit coupling that will break silently.

## Coupling Overview

| Integration | [Strength](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) | [Distance](https://coupling.dev/posts/dimensions-of-coupling/distance/) | [Volatility](https://coupling.dev/posts/dimensions-of-coupling/volatility/) | [Balanced?](https://coupling.dev/posts/core-concepts/balance/) |
| ----------- | --- | --- | --- | --- |
| Orchestrator tool contract: `orchestrator_grants` / `cockpit.execute_orchestrator_tool` ↔ `orchestrator_runtime` / `orchestrator_mcp` | [Functional](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) (duplicated declarations) | High (brain ↔ worker process, HTTP) | High (new core lane; tools will grow) | **No — Issue 1** |
| Continuation retry loop (`cockpit._continue_child_watch`) ↔ `worker/sessions.reserve_turn` error text | Implicit [functional](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) | High (HTTP boundary) | Medium-high (just introduced) | **No — Issue 2** |
| Sub-responsibilities co-located inside `connectors/cockpit.py` | Low (between the parts) | None (one 2,506-line module) | High (hottest file in the repo) | **No (low cohesion) — Issue 3** |
| `orchestration/api.py` ↔ `connectors/cockpit.py` | [Model](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) | Low (same process, same team) | Medium | Borderline — Issue 4 |
| Cockpit connector ↔ worker session API (turns/events via `worker_session_contract`) | [Contract](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) | High (HTTP) | High | **Yes** |
| `mint_orchestrator_grant` (connector) ↔ `resolve_orchestrator_grant` (API) | [Contract](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) (signed token schema, one module) | Low | Low (generic crypto) | **Yes** |
| MCP bridge (`orchestrator_mcp`) → `/v1/orchestrator-tools/...` endpoint | [Contract](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) (grant-scoped HTTP) | High | High | **Yes** |

The three "Yes" rows are worth calling out as design wins: the worker reaches Jarvis only over HTTP with a scoped bearer grant (the repo's network-boundary constraint holds), turn/event consumption uses the shared `worker_session_contract` published language, and the grant format lives in exactly one module used by both mint and resolve sides.

## Issue 1: The orchestrator tool contract is declared in four places

**Integration**: `orchestration/orchestrator_grants.py` / `connectors/cockpit.py` (brain side) ↔ `worker/orchestrator_runtime.py` / `worker/orchestrator_mcp.py` (worker side)
**Severity**: Significant

### Knowledge Leakage

The knowledge "which tools exist on the orchestrator surface, and what arguments they take" is [shared functional knowledge](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) — every participant must agree on it for a tool call to succeed. Today it is re-declared independently four times:

1. `orchestrator_grants.ORCHESTRATOR_TOOL_NAMES` — a frozenset baked into every signed grant.
2. `orchestrator_runtime.ORCHESTRATOR_TOOL_NAMES` — a tuple used to build the provider `allowed_tools` list.
3. The four `@mcp.tool` function signatures in `orchestrator_mcp.py` — which also encode each tool's argument schema.
4. The four `_*_tool(...)` registrations in `cockpit.execute_orchestrator_tool` — which encode the *authoritative* argument schemas a second time.

Nothing ties these together — not an import, not a test that diffs them. The repo already has the right pattern for exactly this situation: `worker_session_contract.py` is the published language for session events, imported by both tiers. Tool names and argument shapes did not get the same treatment.

### Complexity Impact

A developer adding a fifth orchestrator tool must know that four files need coordinated edits — and there is no compiler error, test failure, or grep-able single symbol that says so. Forgetting the grants frozenset produces the most confusing failure: the tool is offered to the agent (runtime list), callable through the bridge (MCP signature exists), served by the API handler — and then rejected with a 403 "grant does not cover this tool" minted two hours earlier. That is a change outcome nobody can predict from reading any one of the four files, which is the definition of [complexity](https://coupling.dev/posts/core-concepts/complexity/) in this model: the shared knowledge exceeds what a maintainer can hold while editing one site.

### Cascading Changes

Every evolution of this lane triggers the cascade: adding a tool (planned — this is the core, [high-volatility](https://coupling.dev/posts/dimensions-of-coupling/volatility/) lane), renaming one, changing an argument (e.g. adding `base_branch` to `spawn_child_work_session` means editing the MCP bridge signature *and* the cockpit tool schema), or versioning the grant payload. Because the four sites sit on both sides of a process/HTTP boundary, the cost of a missed site is a runtime integration failure, not a local one — high [distance](https://coupling.dev/posts/dimensions-of-coupling/distance/) multiplies the price of the duplicated knowledge.

### Recommended Improvement

Introduce a shared contract module — `orchestrator_tool_contract.py`, sibling to `worker_session_contract.py` — owning the tool names (one constant) and, ideally, each tool's argument-schema as data. `orchestrator_grants`, `orchestrator_runtime`, and `cockpit.execute_orchestrator_tool` import it; `orchestrator_mcp` can generate its bridge tools from it (FastMCP supports dynamic registration) or at minimum assert against it at startup. This converts the [functional coupling](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) into [contract coupling](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/), which is the appropriate strength for the high distance involved. Trade-off: one more module and slightly more indirection in the bridge; worthwhile because this is the highest-volatility surface in the PR and the contract is guaranteed to change. (A cheaper first step: a unit test asserting the four declarations are identical.)

## Issue 2: Retry semantics depend on worker error-message strings

**Integration**: `connectors/cockpit.py` (`_continue_child_watch` retry loop) → `worker/sessions.py` (`reserve_turn` error text)
**Severity**: Significant

### Knowledge Leakage

The continuation loop decides whether a failed parent turn is retryable by substring-matching the worker's error message: `"active turn"`, `"already has an active turn"`, `"does not accept new turns"`. These strings are formatted inside `reserve_turn` on the far side of the HTTP boundary and travel back as a free-text `error` field. The connector therefore holds implicit knowledge of the worker's *human-facing wording* — an implementation detail, not an interface. This is [implicit coupling at near-intrusive strength](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) hiding inside what looks like a clean contract.

### Complexity Impact

The dependency is invisible at both ends. A worker-side developer editing an error message for clarity has no signal that a brain-side retry loop parses it; a brain-side reader cannot tell which worker versions produce matching strings. Any intermediary that rewrites errors (the existing `public_error_message` redaction, a proxy, i18n) silently changes behaviour from "retry for up to `job_timeout_s`" to "fail the watch immediately" — and the failure mode (a review parent that never resumes after its children finish) manifests far from the cause.

### Cascading Changes

Rewording one f-string in `worker/sessions.py` forces a matching edit in `connectors/cockpit.py`, but nothing enforces it — the cascade fails open. Given the two modules deploy separately in Phase 2 (worker on the fleet, brain on the mini), version skew makes silent breakage a *when*, not an *if*.

### Recommended Improvement

Add a machine-readable code to the worker's 409 response (`{"ok": false, "error": "...", "code": "turn_active"}`) and define the code constants in `worker_session_contract.py`. The retry loop keys on the code; the message stays free for humans. This is a strict strength reduction — from implicit string knowledge to an explicit [published language](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) — at trivial cost (one field, backward compatible since old workers simply omit it and the string match can remain as a fallback during transition).

## Issue 3: `connectors/cockpit.py` is a 2,506-line change magnet with low cohesion

**Integration**: co-located sub-responsibilities within `connectors/cockpit.py`
**Severity**: Significant

### Knowledge Leakage

The inverse problem of Issues 1–2: components that share *little* knowledge are pinned at zero [distance](https://coupling.dev/posts/dimensions-of-coupling/distance/). One module now contains: the `CockpitThread` dataclass and JSON index (persistence), three distinct turn strategies (assistant/BrainSession, workspace, and the new orchestrator lane), the worker HTTP client helpers, the orchestrator tool factories and their schemas, grant minting calls, and the background child-watch threads. These parts interact mostly through well-defined seams (the index API, `_post_worker_json`), i.e. their mutual [integration strength](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) is already low — which is precisely why their co-location is imbalance rather than cohesion. Low strength + low distance is the [big-ball-of-mud gradient](https://coupling.dev/posts/core-concepts/balance/).

### Complexity Impact

Project memory shows this is the second-most-modified file in the repo (39 changes), and this PR alone added ~370 lines to it. Every feature in the Cockpit lane — whatever its actual subject — lands here, so every developer must page the whole module's concerns into working memory to change any one of them (well past the 4±1 working-memory budget). Merge conflicts concentrate here, and unrelated features become [lifecycle-coupled](https://coupling.dev/posts/dimensions-of-coupling/distance/): a change to watch-retry timing and a change to thread projection ship, review, and conflict together.

### Cascading Changes

Not cascading *logic* changes — cascading *process* costs: conflicts, review blast radius, and accidental reuse of module-private helpers across unrelated strategies (e.g. the orchestrator turn reaching directly into `thread.workspace` dict internals shared with the workspace strategy). As the orchestrator lane grows (more engines, more tools — confirmed direction), the file's growth is superlinear in pain.

### Recommended Improvement

Increase distance where strength is already low — the safe direction of the [balance rule](https://coupling.dev/posts/core-concepts/balance/). Extract, in order of value: (1) the orchestrator lane (`_orchestrator_turn`, `_ensure_orchestrator_session`, `_wait_for_orchestrator_turn`, `execute_orchestrator_tool`) into `connectors/cockpit_orchestrator.py`; (2) `CockpitThread` + `CockpitThreadIndex` into a persistence module. Both already interact with the rest through narrow seams, so this is file movement, not redesign. Trade-off: import churn and a slightly deeper package; worthwhile because this file is where the volatility is and the split makes each future PR touch one strategy instead of the union.

## Issue 4: `orchestration/api.py` ↔ `connectors/cockpit.py` bidirectional package coupling

**Integration**: `orchestration/api.py` → `connectors/cockpit.py` (six imported symbols) and `connectors/cockpit.py` → five `orchestration.*` modules
**Severity**: Minor

### Knowledge Leakage

The API imports cockpit *internals* — `CockpitThread`, `CockpitThreadIndex`, `THREAD_INDEX_FILENAME`, and now `execute_orchestrator_tool` — while cockpit imports `orchestration.store`, `service`, `workers`, `redaction`, and the new `orchestrator_grants`. The two packages share their [domain models](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) directly in both directions. Notably, the repo's architecture test (`test_orchestration_and_connectors_reach_brain_only_via_facade`) polices both packages' access to `brain/` but is silent on this pair — the discipline that exists at the brain boundary has no equivalent here.

### Complexity Impact

Today this is largely tolerable: same process, same team, and [model coupling](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) at low [distance](https://coupling.dev/posts/dimensions-of-coupling/distance/) is balanced by the model's own rule. The cost is directional ambiguity — there is no single owner of the thread concept, so changes to `CockpitThread`'s shape must be checked against two packages' usage, and the new `/v1/orchestrator-tools` handler makes the API's behaviour depend on cockpit tool internals it cannot see.

### Cascading Changes

The risk is future-tense: Phase 2 moves tiers across machines by design. If the Cockpit API and the connector ever land on different sides of a boundary, this bidirectional model sharing becomes high-strength/high-distance overnight — the [distributed-monolith](https://coupling.dev/posts/core-concepts/balance/) pattern. Until then, cascades are cheap (same-process refactors).

### Recommended Improvement

Accept the imbalance for now — [volatility](https://coupling.dev/posts/dimensions-of-coupling/volatility/) of the *pairing* (as opposed to the lane's features) is moderate and the distance is minimal, so the balance rule tolerates it. Two cheap hardening steps: (a) extend the existing architecture test to pin the allowed import list between these two packages so growth is deliberate rather than accidental; (b) when Issue 3's extraction happens, let the API import the new orchestrator module rather than deepening its reach into `cockpit.py`. Full decoupling (a facade like `brain/facade.py`) is *not* recommended yet — that would add distance without reducing strength, the exact trade the model warns against.

---

_This analysis was performed using the [Balanced Coupling](https://coupling.dev) model by [Vlad Khononov](https://vladikk.com)._
