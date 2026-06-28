# Modularity Review

**Scope**: Branch `codex/architecture-boundaries`, reviewed against `origin/main...HEAD`.
**Date**: 2026-06-29

## Executive Summary

This branch covers the original [balanced coupling](https://coupling.dev/posts/core-concepts/balance/) problem correctly for the runtime/tool boundary. The previous high-strength dependency from tools into `jarvis.brain` has been replaced with `jarvis.runtime`, a neutral [contract coupling](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) point for request context, capability checks, and tool registration. The user profile, scheduler, and front-matter extractions also move cross-cutting logic into stable owners, which lowers cascading-change risk across setup, WhatsApp, tools, skills, and brain orchestration.

The review initially found one smaller imbalance: skill markdown parsing reused the user profile store's front-matter parser. That issue has been fixed by moving the generic parser to `jarvis.frontmatter`, leaving `jarvis.users` as the owner of user-profile behavior and `brain.skills` as the owner of skill recipe behavior. The branch now satisfies the reviewed [balance rule](https://coupling.dev/posts/core-concepts/balance/) cases: high-volatility concerns either stay close to their owner or communicate through low-strength contracts.

## Coupling Overview

| Integration | [Strength](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) | [Distance](https://coupling.dev/posts/dimensions-of-coupling/distance/) | [Volatility](https://coupling.dev/posts/dimensions-of-coupling/volatility/) | [Balanced?](https://coupling.dev/posts/core-concepts/balance/) |
| --- | --- | --- | --- | --- |
| `brain` and `tools` through `jarvis.runtime` | [Contract](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) | Package boundary inside one service | High: tool surface, request context, and capability policy evolve with assistant behavior | Yes. Low strength offsets module distance. |
| `setup`, WhatsApp, identity, and profile tools through `jarvis.users` | [Model](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) / [contract](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) | Package boundary inside one service | High: household profiles, account references, pairing, and facts are active domain concepts | Mostly yes. The shared model has a single owner and callers use focused helpers. |
| `brain.skills`, `jarvis.users`, and `jarvis.frontmatter` | [Contract](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) | Package boundary inside one service | High: skill recipes and user profile schemas can change independently | Yes. Shared mechanics are neutral; schema ownership stays local. |
| `brain` and alarm tools through `jarvis.scheduling` | [Model](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) | Package boundary inside one service | Low to moderate: alarm timing rules are narrow and stable | Yes. Shared concepts are cohesive and implementation-independent. |
| Intercom, browser, worker, and MCP lanes | [Contract](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/) | Runtime/process boundary | High implementation volatility, lower protocol volatility | Yes, assuming contracts remain the boundary. |

## Branch Coverage Notes

The runtime extraction is the strongest part of the branch. `src/jarvis/runtime.py` explicitly imports no brain, tool, or infrastructure packages and owns `RequestContext`, `CapabilityError`, `Tool`, `ToolRegistry`, and execution gating. The new architecture regression test asserts that files under `src/jarvis/tools` do not import `jarvis.brain`, which directly protects the boundary the branch set out to fix.

The user-store extraction is also largely aligned with [modular design](https://coupling.dev/posts/core-concepts/modularity/). `src/jarvis/users.py` is now the owner for profile front matter, WhatsApp numbers, account references, and managed facts. `src/jarvis/setup.py` and `src/jarvis/connectors/whatsapp.py` depend on that owner instead of reaching into brain internals, so onboarding and connectors no longer need to know brain-private identity/profile modules.

The scheduler extraction is balanced. `src/jarvis/scheduling.py` holds alarm/timer primitives while brain and tools use compatibility imports or the neutral module. This is model sharing, but the model is small, cohesive, and lower volatility than the surrounding assistant workflow.

The front-matter extraction closes the one issue found during this review. `src/jarvis/frontmatter.py` now owns the generic flat markdown parser, `src/jarvis/users.py` keeps user-specific profile and fact behavior, and `src/jarvis/brain/skills.py` imports the parser directly from the neutral module. `tests/unit/test_architecture_boundaries.py` now prevents `brain.skills` from depending on `jarvis.users`.

## Resolved Issue 1: Skill Parsing Was Coupled To The User Profile Store

**Severity**: Significant

**Integration**: `src/jarvis/brain/skills.py` imported `parse_front_matter` from `src/jarvis/users.py`; it now imports from `src/jarvis/frontmatter.py`.

**Knowledge Leakage**

`jarvis.users` describes itself as the module through which code reads or updates `users/*.md`. Before the follow-up fix, its `parse_front_matter` function was also used by `brain.skills` to read skill recipe metadata. That made the user profile store responsible for a skill-file format it did not own.

This is stronger than simple [contract coupling](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/). The parser embodies shared assumptions about markdown front matter syntax, list handling, scalar coercion, and schema tolerance. Because those assumptions are used for both user profiles and skill recipes, a change intended for one format can change the other.

**Complexity Impact**

Before the fix, developers changing user account/profile metadata had to consider skill recipe parsing behavior, and developers changing skill recipes had to reason about the user store. That increased cognitive load and made change outcomes less predictable, which is the [complexity](https://coupling.dev/posts/core-concepts/complexity/) symptom the branch is trying to reduce.

**Cascading Change Risk**

Likely future changes include richer skill parameters, stricter user profile validation, account-binding metadata changes, multiline front-matter values, or a move to a real YAML parser. With the old dependency, any of those could have required coordinated changes and tests across `jarvis.users`, `brain.skills`, setup, identity resolution, WhatsApp pairing, and skill loading.

**Resolution Applied**

The generic front-matter mechanics were moved to `jarvis.frontmatter`. This reduces the integration back to [contract coupling](https://coupling.dev/posts/dimensions-of-coupling/integration-strength/): `users` owns user-profile schema decisions, `brain.skills` owns skill schema decisions, and only format-neutral parsing mechanics are shared.

The architecture regression test now prevents `src/jarvis/brain/skills.py` from depending on `jarvis.users`.

## Verification

Verified with `uv run pytest tests/unit -q`, `uv run ruff check src/jarvis/frontmatter.py src/jarvis/users.py src/jarvis/brain/skills.py tests/unit/test_architecture_boundaries.py`, and `git diff --check`.

---

_This analysis was performed using the [Balanced Coupling](https://coupling.dev) model by [Vlad Khononov](https://vladikk.com)._
