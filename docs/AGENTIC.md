# AGENTIC.md — the agentic engineering cycle

**Status: design / decomposition, with the first CLI-checkable orchestration
foundation under construction.** This is the shared map we argue over and fill
in. It describes the *whole* capability — commanding real engineering work by
voice/text and having it done across machines and agents — as a **single closed
cycle**, then pulls that cycle apart into the primitives that power it. The
missing primitives are called out, but always *in their place in the loop*, never
as a standalone backlog: the point is the complete cycle, including them.

> The north-star use: *"review PR 27 on `<repo>` with Opus and GPT-5.5, combine,
> and write the replies in the PR"* — or *"take the Linear ticket, build the
> feature on my personal laptop, raise the PR, tell me when it's up"* — said to
> Jarvis over any control surface (a voice intercom on a Mac or Pi, WhatsApp, a
> text console), run unattended, picked up locally later. These are not features;
> they are *programs* run on the machine this doc specifies.

## Honour the two hard constraints

Everything below lives under AGENTS.md's two constraints:

1. **Network boundary everywhere.** Engines, targets, trackers, and forges are
   reached over HTTP/CLI from config — no in-process shortcuts. A "target" on
   another laptop is the same code path as the local worker, only a different
   host. This is what makes "run it on my personal laptop / on Hive / on
   serverless" a config choice, not a rewrite.
2. **The hot path never blocks.** The voice turn that *starts* a unit of work
   returns immediately ("on it"); all execution, review, and landing happen on
   the cold/background path and report back proactively. No primitive here may be
   awaited on the hot path.

## Work sources, commands, and orchestration truth

The Symphony-shaped addition to Jarvis is **work-source orchestration**: Linear,
GitHub, and direct voice/text requests are intake sources for the same loop, not
separate product features.

```
WorkSource -> WorkCommand -> OrchestrationRun -> ExecutionEnvelope
  -> WorkerJob(s) -> Branch/PR/comment artifacts -> WorkSource update -> Report
```

**Jarvis, not the tracker, owns orchestration state.** Linear/GitHub are public
or human-facing reflections: they can rank, describe, claim, link, and comment on
work, but the local Jarvis run graph is the operational truth because one unit of
work may span many tickets, many jobs, many PRs, or no tracker item at all.

### Work sources

Work sources expose common verbs over different systems:

- `list` / `next` — inspect or select candidate work.
- `claim` — best-effort public reflection that Jarvis has picked something up.
- `comment` / `link_pr` — public-safe status and handoff updates.
- `inspect_pr_comments` — read review threads/comments as work inputs.

GitHub may be both a **work source** (issues, PR comments, failing checks) and a
**forge** (branch push, PR creation, review comment, merge). These are different
authority levels: reading an issue is not the same thing as pushing to GitHub.

### WorkCommand

Natural language is first converted into a structured `WorkCommand`. The LLM may
produce this structure directly; deterministic code validates and executes it.
Adapters never parse casual English themselves.

Examples:

```json
{"operation":"inspect_work","source":"github","kind":"issue","autonomy":"read_only"}
{"operation":"start_next_work","source":"linear","kind":"ticket","autonomy":"start_if_unambiguous"}
{"operation":"inspect_pr_comments","source":"github","kind":"pull_request","autonomy":"read_only"}
{"operation":"resume_run","source":"jarvis","autonomy":"start_if_unambiguous"}
```

Richer examples, preserving the intent of the user's words:

```json
{
  "operation": "start_next_work",
  "source": "linear",
  "kind": "ticket",
  "filters": {
    "assignee": "me",
    "status": "ready"
  },
  "autonomy": "start_if_unambiguous",
  "target_worker_id": "macbook-worker",
  "start": true
}
```

```json
{
  "operation": "inspect_work",
  "source": "github",
  "kind": "issue",
  "filters": {
    "repo": "roughcoder/jarvis",
    "state": "open"
  },
  "autonomy": "read_only",
  "start": false
}
```

Default command semantics:

- `check`, `show`, `list`, `summarize` -> read-only inspection.
- `get`, `take`, `pick up`, `start`, `work on` -> select, claim, and start when
  unambiguous and authorised.
- `fix`, `address`, `handle` -> execute against a selected issue/PR/comment.
- `resume`, `continue` -> find the existing run/session before starting new work.
- `blocked`, `stalled`, `what's running` -> inspect Jarvis run graph state.

### Run graph

The durable unit is an `OrchestrationRun`, not a one-ticket-one-PR record:

```
OrchestrationRun
  -> WorkItems[]     # primary, related, blocks, blocked_by, follow_up
  -> WorkerJobs[]    # worker_id + job_id + engine session/resume handle
  -> Artifacts[]     # branches, PRs, comments, evidence
  -> Events[]        # append-only audit trail
  -> ChildRuns[]     # campaigns or split delivery
```

Rules:

- One active **primary owner** run per work item.
- A run may own many work items, jobs, and artifacts.
- A work item may appear as related context in other runs.
- Large objectives become parent campaign runs with bounded child runs.
- `events.jsonl` is the audit trail; `run.json` is the current view.

Example run graph for one ticket producing one PR:

```json
{
  "run_id": "run_20260629_abcd1234",
  "parent_run_id": null,
  "objective": "Add worker heartbeat status",
  "phase": "running",
  "status": "active",
  "work_items": [
    {
      "role": "primary",
      "item": {
        "source": "linear",
        "id": "ENG-42",
        "title": "Add worker heartbeat status",
        "url": "https://linear.app/example/issue/ENG-42",
        "repo": "roughcoder/jarvis",
        "kind": "ticket",
        "status": "In Progress",
        "labels": ["worker"]
      }
    }
  ],
  "jobs": [
    {
      "worker_id": "macbook-worker",
      "job_id": "job_abc123",
      "status": "running",
      "engine": "codex",
      "session_id": "codex-session-id",
      "branch": "jarvis/eng-42-worker-heartbeat"
    }
  ],
  "artifacts": [
    {
      "type": "pull_request",
      "url": "https://github.com/roughcoder/jarvis/pull/123",
      "status": "draft",
      "public": true
    }
  ],
  "child_run_ids": []
}
```

Example run graph for many tickets batched into one run:

```json
{
  "run_id": "run_batch_cleanup",
  "objective": "Batch the next three cleanup tickets",
  "work_items": [
    {"role": "primary", "item": {"source": "linear", "id": "ENG-42", "title": "Clean worker status"}},
    {"role": "primary", "item": {"source": "linear", "id": "ENG-43", "title": "Clean job output"}},
    {"role": "primary", "item": {"source": "linear", "id": "ENG-44", "title": "Clean CLI copy"}}
  ],
  "artifacts": [
    {"type": "pull_request", "url": "https://github.com/roughcoder/jarvis/pull/124"}
  ]
}
```

Example parent campaign that creates multiple child PR runs:

```json
{
  "run_id": "run_campaign_bugs",
  "objective": "Spend two hours clearing GitHub bugs",
  "phase": "running",
  "child_run_ids": ["run_bug_1", "run_bug_2", "run_bug_3"],
  "events": [
    {"type": "campaign_started", "message": "Budget: 120 minutes, max 5 items"},
    {"type": "campaign_children_created", "message": "Created 3 child run(s)"}
  ]
}
```

### Scheduler and schedules

There are two schedulers:

- **Work scheduler:** selects, filters, ranks, claims, starts, or resumes work.
- **Time scheduler:** fires stored structured `WorkCommand`s at configured times.

Scheduled work stores structure, not raw prose. A phrase like "every weekday at
9am, get the next Linear ticket" is translated once into:

```json
{
  "trigger": "09:00 weekdays Europe/London",
  "command": {"operation":"start_next_work","source":"linear","kind":"ticket"},
  "policy": {"skip_if_active":true,"max_concurrent_runs":1,"report_on_no_work":true}
}
```

Concrete schedule record:

```json
{
  "schedule_id": "sched_weekday_linear",
  "name": "Weekday Linear pickup",
  "enabled": true,
  "timezone": "Europe/London",
  "hour": 9,
  "minute": 0,
  "weekdays": [0, 1, 2, 3, 4],
  "mode": "one_shot",
  "command": {
    "operation": "start_next_work",
    "source": "linear",
    "kind": "ticket",
    "filters": {
      "assignee": "me",
      "status": "ready"
    },
    "autonomy": "start_if_unambiguous",
    "target_worker_id": "macbook-worker",
    "start": true
  },
  "policy": {
    "max_concurrent_runs": 1,
    "skip_if_active": true,
    "catch_up": "skip",
    "report_on_no_work": true,
    "public_write_mode": "draft_then_confirm"
  }
}
```

Concrete campaign policy:

```json
{
  "mode": "campaign",
  "source": "github",
  "filters": {
    "repo": "roughcoder/jarvis",
    "label": "bug"
  },
  "budget": {
    "max_items": 5,
    "max_duration_minutes": 120,
    "max_concurrent_runs": 1
  },
  "stop_when": ["queue_empty", "budget_exhausted", "blocked", "human_needed"]
}
```

Two modes are first-class:

- **Recurring one-shot:** each trigger starts at most one item.
- **Campaign:** a parent run drains a queue under limits: max items, duration,
  concurrency, and stop conditions (`queue_empty`, `budget_exhausted`, `blocked`,
  `human_needed`).

### Authority policy

Authority is separate from work discovery and machine capability:

```
Work source says: what work exists
Worker profile says: what can run
Principal/device says: who is asking and what they may authorize
Repo policy says: what public writes are allowed
```

Settings may make defaults stricter or choose behaviour (`branch_only`,
`draft_pr`, `ready_pr`, `confirm_before_pr`), but settings do not grant authority
by themselves. Scheduled work re-checks authority when it fires. Merge and release
remain explicit high-trust actions, never default automation.

Example repo/forge policy:

```json
{
  "repo": "roughcoder/jarvis",
  "public_write_mode": "draft_then_confirm",
  "pull_request_mode": "draft_pr",
  "allow_branch_push": true,
  "allow_pr_comments": "confirm",
  "allow_automerge": false,
  "blocked_actions": ["forge.github.merge", "release.trigger"]
}
```

Example capability vocabulary:

```json
{
  "read": [
    "work.linear.read",
    "work.github.issues.read",
    "work.github.pr.read",
    "orchestration.runs.read"
  ],
  "write": [
    "worker.job.start",
    "work.linear.write",
    "forge.github.branch.push",
    "forge.github.pr.create",
    "forge.github.pr.comment",
    "orchestration.schedules.write"
  ],
  "high_risk": [
    "forge.github.merge",
    "release.trigger",
    "secrets.read",
    "public.write.autonomous"
  ]
}
```

### ExecutionEnvelope

The handoff from scheduler to worker is an `ExecutionEnvelope`:

- selected work items and objective
- named `worker_id` target, not raw machine/host details
- repo/workspace policy
- `engine` / `engine_strategy`
- allowed actions
- verification plan
- landing policy

Workers receive the envelope and stay inside it. They do not rediscover or widen
authority; if they need more, the run becomes `needs_human`.

Example worker profile:

```json
{
  "worker_id": "macbook-worker",
  "display_name": "MacBook worker",
  "status": "online",
  "capabilities": ["git", "python", "uv", "browser", "codex", "claude"],
  "capacity": {
    "max_concurrent_jobs": 1,
    "current_jobs": 0
  },
  "authority_tags": ["owner-private"],
  "agent": "codex",
  "default_engine": "codex",
  "supported_engines": ["codex", "claude"]
}
```

Private connection details such as hostnames, paths, and tokens live in local
config only; they are not copied into public tracker comments or AGENTIC records.
`worker_id` answers where the job runs; `engine` answers which coding CLI or
agent implementation runs inside that worker. Workers own their installed
Codex/Claude credentials and advertise supported engine ids over `/health`.

Example execution envelope:

```json
{
  "run_id": "run_20260629_abcd1234",
  "repo": "roughcoder/jarvis",
  "base_ref": "main",
  "branch_name": "jarvis/eng-42-worker-heartbeat",
  "worker_id": "macbook-worker",
  "engine": "claude",
  "engine_strategy": "single",
  "allowed_actions": ["worker.job.start", "forge.github.branch.push", "forge.github.pr.create"],
  "prompt": "Follow AGENTS.md, read the ticket, implement the change, verify it, and report evidence.",
  "verification": {
    "minimum_rung": "repo_native_plus_task_proof",
    "repo_native": true,
    "task_proof": "Boot the app and verify the changed flow in a browser. Record the URL, interaction path, and observed result.",
    "suggested_commands": [],
    "evidence_required": [
      "repo checks followed",
      "commands run",
      "observed behavior",
      "known gaps"
    ]
  },
  "landing": {
    "mode": "draft_pr",
    "public_write_mode": "draft_then_confirm",
    "allow_comments": "confirm",
    "allow_merge": false
  }
}
```

Later multi-engine compare/review mode should keep one parent run and create
multiple worker jobs under it, rather than overloading one job:

```json
{
  "run_id": "run_review_panel",
  "engine_strategy": "compare",
  "jobs": [
    {"worker_id": "macbook-worker", "engine": "codex", "status": "running"},
    {"worker_id": "macbook-worker", "engine": "claude", "status": "running"}
  ]
}
```

Verification has two layers:

- **Repo-native gates:** `AGENTS.md`, README, `docs/TESTING.md`, package scripts,
  Makefile, CI conventions.
- **Task-specific proof:** natural-language requirements such as "boot the app
  and verify the changed flow in a browser; report the URL, path, and result."

### Observability and reporting

Jarvis should be quiet while work is healthy, clear when work finishes, and loud
only when it needs a human. Default reporting is terminal-only, with pull status
available any time:

- `what's running?`
- `what's blocked?`
- `resume that ticket`
- `show the last run`

Public tracker updates are sanitized summaries only: status, PR link, high-level
verification, and public-safe blockers. Private local paths, hostnames, raw logs,
tokens, and engine session IDs stay local.

Example append-only event:

```json
{
  "time": "2026-06-29T09:15:00Z",
  "type": "verification_evidence_added",
  "run_id": "run_20260629_abcd1234",
  "message": "Ruff and focused worker tests passed",
  "data": {
    "commands": [
      "uv run ruff check src/ tests/ scripts/generate_release_notes.py",
      "uv run pytest tests/unit/test_worker_actions.py -q"
    ]
  }
}
```

Example terminal report payload:

```json
{
  "run_id": "run_20260629_abcd1234",
  "state": "handoff",
  "summary": "ENG-42 is ready for review.",
  "worker_id": "macbook-worker",
  "branch": "jarvis/eng-42-worker-heartbeat",
  "pr_url": "https://github.com/roughcoder/jarvis/pull/123",
  "verification": "ruff passed; focused unit tests passed; browser check not applicable",
  "known_gaps": [],
  "resume": {
    "job_id": "job_abc123",
    "engine": "codex",
    "session_id": "codex-session-id"
  }
}
```

## Proportionality — the loop chooses the lightest sufficient depth

**The governing principle, and the one that keeps this from being absurd:** the
cycle below is the *full range* of what the machine can do, **not** what fires on
every request. Most turns use a thin slice. The loop must always pick the
**cheapest verification — and the fewest stages — sufficient for this change**,
and reach for depth only when the change warrants it.

Verification depth scales with the change, not with the machine's ceiling:

| Change | Verification rung (P12/P13) |
|---|---|
| docs · comment | doc lint / spell / link-check **if configured** (no build) |
| config | config validation, or lint/build if the config feeds one |
| code: logic · refactor · internal API | unit + integration tests |
| feature · UI · public API · data | **exercise the real app** (browser/sim/boot+probe) — the expensive, flaky, opt-in-by-change rung |

The same restraint applies to the rest of the cycle, not just verification:

- **Stages are skippable.** A one-line fix needs no durable work item (P6), no
  ensemble (P3), no explicit stage 3 Plan step — Intake → Execute → (cheap verify)
  → Land. The 10 stages are the *maximal* path; the loop collapses them when it
  can.
- **Ensemble is opt-in.** One engine is the default; fan out to a panel only when
  the stakes justify the latency/cost (a risky review, a wide design space).
- **Deep app-exercise is opt-in-by-change-type**, and the most expensive thing
  here — treat it as the rung you climb *to*, never the rung you start on.

Rule of thumb: **a turn should feel as light as the change is small.** If a typo
fix triggers a simulator boot and a three-model panel, the loop is wrong — not
because the capability is wrong, but because it failed to be proportional.

## Safety invariants (hard rules, not tensions)

Some boundaries are safety-critical and must be settled *before* implementation,
not parked as open tensions. These are invariants the design must hold:

- **I1 · Isolation is absolute; cleanup only deletes worker-owned paths.** No
  engine ever edits a user-supplied path *in place*, and cleanup must only ever
  remove paths the worker created. A git input → a fresh worktree+branch off
  HEAD. A **non-git input → copied into worker-owned scratch, or refused** —
  never run in place. ⚠️ *The current worker violates this:* a non-git `repo`
  runs in place (`src/jarvis/worker/actions.py:191-192`) and cleanup then
  `rmtree`s that path because `branch` is `None`
  (`src/jarvis/worker/actions.py:206-211`) — it can delete the user's own
  directory. This is a **P0 bug to fix**, and the invariant that prevents its
  whole class. (See P4.)
- **I2 · Panels are read-only unless every engine is independently isolated.** A
  multi-engine ensemble (P3) may run engines concurrently against the **same**
  workspace *only* when they are read-only (e.g. model review of a diff).
  **Writing** CLI engines in a panel each get their **own worktree/branch**, and
  convergence must define an explicit **merge or selection** strategy — never N
  writers racing on one tree. (See P3.)
- **I3 · Authority ≠ machine capability.** A target's *resources/toolchains*
  (Xcode, browser, docker — doctor-probed) are a different axis from
  *principal-bound credentials and write authority* (who `gh` is authenticated
  as, which signing identity, whether this request may push). Resources come from
  the machine (P2); authority comes from the trust gate and the request's
  principal (P9). A laptop having `gh` installed never implies it may write as
  you. (See P2, P9.)
- **I4 · Public forge writes default to draft-then-confirm.** Voice/text-
  originated **public** writes — pushing a shared branch, opening a PR, posting PR
  comments/reviews — require an explicit `forge.write.public` capability **and**,
  by default, a draft-then-confirm step before the write lands. Autonomous public
  writes are opt-in per (principal × repo), decided up front — not a late policy
  knob. Private/local actions (worktree commits, local branches) stay autonomous.
  (See P5, P9.)

The north-star example ("write the replies in the PR" unattended) is therefore a
*configured* exception to I4, granted ahead of time — not the default.

---

## Part 1 — The complete cycle

A unit of work flows through ten stages. The loop is closed: stage 10 feeds back
into stage 1 (iterate on a review, follow-up ticket, learned skill).

```
        ┌────────────────────────────────────────────────────────────┐
        │                                                            ↓ │
   (1) INTAKE → (2) FRAME → (3) PLAN → (4) PROVISION → (5) EXECUTE →   │
   command       a durable    pick       resolve a       run engine(s)│
   arrives by    spec/work    engine(s)  target +        on the target│
   voice/text    item         + target   workspace       in the       │
   (identity +   (issue/      + recipe   (clone/         workspace     │
   trust         Linear/                 worktree/                     │
   resolved)     spec)                   branch)              ↓        │
        ↑                                                              │
        │   (10) LEARN ← (9) HANDOFF ← (8) REPORT ← (7) LAND ← (6) CONVERGE
        │   job record    branch/PR is   proactive    push, open   synthesise
        │   + session_id  the unit of    push back     PR, post     ensemble +
        └── + memory +    handoff;       to the        comments/    self-review
            saved skill   pick up        control       review       loop until
                          locally        surface                    clean
```

**The stages, in words:**

1. **Intake.** A request lands on a *control surface* — a channel on a device: a
   voice intercom (Mac, Pi), WhatsApp, or a text console. Identity and trust are
   resolved (the *device* carries the trust tier) → a `RequestContext` with a
   capability set. A request may be precise ("review PR 27") or vague ("sort out
   the flaky tests") — vagueness is resolved in Frame, not refused.
2. **Frame.** Turn intent into a **durable work item**: a GitHub issue, a Linear
   ticket, or an inline spec. This is where *"decide complex work"* lives —
   reading trackers, the repo, and memory to scope it. The work item is the
   anchor a PR will later close, and the thing a human can inspect mid-flight.
3. **Plan.** Choose **engine(s)** (Opus, GPT-5.5, Codex, Claude, Factory,
   Cursor…), **target(s)**, and the **recipe** (single build, ensemble review,
   plan-then-build). Target choice is **capability-matched**, not free: a Swift
   build needs a target whose profile has Xcode + a simulator; a webapp E2E needs
   one with a browser. The plan declares what the work *requires*; routing picks a
   target that *can* (or reports that none can). For simple commands this is
   implicit; for complex ones it's an explicit step.
4. **Provision.** Resolve the chosen target and prepare an **isolated
   workspace** — clone the repo if missing, cut a worktree on a fresh branch off
   HEAD: never the user's live checkout. A non-git input is **copied into
   worker-owned scratch**, never run in place (invariant I1; see P4).
5. **Execute.** Run the engine(s) on the target in the workspace. One engine, or
   **an ensemble in parallel** (panel). Long-running, async, tracked as a job.
   Writing engines in a panel must each get their own isolated worktree
   (invariant I2) — only read-only review may share one workspace.
6. **Converge.** If an ensemble ran, **synthesise** the outputs. Run the
   **self-correction loop** — exercise the running app via the harness for its
   type (P13), review, and iterate until there are no accepted actionable
   findings and the app observably works (OpenClaw's issue→PR discipline, deepened
   to real-app verification). See "The inner loop" below.
7. **Land.** **Forge verbs**: commit, push the branch, open the PR, post the
   review — top-level summary and/or inline line-comments / replies.
8. **Report.** Proactive push back to the surface that asked: "done — PR #N is
   up, here's the gist." Idle-aware, quiet-hours-aware (see NOTIFICATIONS.md).
9. **Handoff.** The **unit of handoff is the branch / PR** — never proprietary
   state. Pick it up locally on any machine with `git fetch`, or iterate
   ("address the review comments") which re-enters the loop at stage 5.
10. **Learn.** Persist the job record + the agent's `session_id` (for
    `codex resume`), update memory, optionally save a reusable **skill**. Feeds
    back into Intake for the next turn.

Governing all ten, continuously: the **trust gate** (deny-by-default; "no
limits" = the owner's full-caps profile on a trusted device) and
**observability** (what's running, logs, traces, resume handles).

### The inner loop — converge before you land (the part OpenClaw nails)

Stages 5→6 are not a straight line; they are a **bounded self-correction loop**,
and it is the single most important thing OpenClaw gets right. An autonomous
build is only trustworthy if it *checks its own work and iterates until clean
before it ever opens a PR* — not "generate once and hope". Zooming into stages
5–7:

```
        ┌──────────────────────────── fix & retry ───────────────────────────┐
        ↓                                                                     │
   (5) EXECUTE ──> EXERCISE THE APP ──> self-review ──> VERIFICATION GATE ────┤
   implement/edit   via the harness      (a review      are we done?          │
                    for THIS app-type    engine — or     · build/tests green?  │
                    on THIS machine:     an ensemble)    · app actually does   │
                    · webapp → boot +                      the new thing,      │
                      drive in browser                     observed?           │
                    · iOS/Swift → run on                  · review: no         │
                      simulator + UI snap                   accepted           │
                    · service/API → boot                    actionable         │
                      + hit endpoints                        findings?         │
                    · CLI → run + assert                   · acceptance        │
                    · library → unit/intg                    criteria met? ────┘
                    (capped by the target's                  │ yes      │ no, but
                     capability profile)                     ↓          │ stuck /
                                                        (7) LAND        │ N exceeded
                                                                        ↓
                                                                ESCALATE: report
                                                                partial branch +
                                                                what's red, ask
                                                                the human (re-enter
                                                                at Intake)
```

The discipline, made explicit (OpenClaw's coding-agent recipe, generalised):

- **Exit predicate.** Land *only* when the Verification gate (P12) is satisfied:
  build/tests pass, **the running app was exercised and observably does the new
  thing**, the review surfaces no *accepted actionable* findings, **and** the work
  item's acceptance criteria are met. "Accepted actionable" is load-bearing — a
  finding the loop judges wrong or out-of-scope is recorded and dismissed, not
  chased forever.
- **Exercise the real app, not just the unit.** Unit tests are the floor, not the
  bar. "Make this change to the webapp" is only verified when the loop *boots the
  webapp and drives it in a browser*; a Swift change when it *runs on a simulator
  and checks the UI*; a service change when it *boots and the endpoint responds*.
  The harness is chosen by **app-type × the target's capability profile** (P13,
  fed by P2) — and what's even *possible* is capped by what the machine can do
  (no Xcode → no simulator run; no browser → no E2E). This is the deepest
  feedback loop, and the one a unit-test-only gate misses.
- **Bounded.** A hard `max_iterations` (and a wall-clock/cost budget). The loop
  must not be able to spin. Hitting the bound is not failure-silent: it
  **escalates** — report the partial branch + what's still failing, and hand back
  to the human (re-enter the cycle at Intake). This is the difference between
  "ran out of time and stopped" and "claimed done when it wasn't".
- **Isolated.** Every iteration runs in the P4 isolated workspace, never the
  user's live tree (OpenClaw's mandatory-isolated-checkout rule). For a git repo
  that's a fresh worktree off HEAD; a non-git dir gets a scratch copy or an
  accepted-risk note (see P4) — never edited in place under the loop.
- **One terminal report.** The loop runs detached (P7) and emits exactly one
  *terminal* completion/failure message with the result + a resume handle (P11)
  — with, for long jobs, optional *coarse* progress updates (e.g. "still going,
  on iteration 3"). What it must not do is chatter per iteration, or interrupt
  unless it needs to ask a question it genuinely cannot answer alone. (Whether
  coarse progress is push or pull is the P7/P11 open question.)

This nested loop sits *inside* the outer cycle. The **outer** loop is
human-driven (stage 9 handoff: "address the review comments" re-enters at stage
5); the **inner** loop is autonomous (converge before landing). Both must exist;
conflating them is what the first draft of this doc got wrong.

---

## Part 2 — Two worked traces

**A. "Review PR 27 on `acme/api` with Opus and GPT-5.5, combine, write the replies."**

| Stage | What happens | Primitive |
|---|---|---|
| Intake | voice intercom (Mac/Pi); owner on a trusted device → full caps | Control surface, Trust gate |
| Frame | Work item *is* the existing PR; fetch its diff + thread | Work item, Forge verbs |
| Plan | engine = `panel([opus, gpt-5.5])`, synth = strong; target = worker; recipe = review | Engine, Ensemble, Target, Recipe |
| Provision | (review-only) checkout the PR head in a worktree for context | Workspace |
| Execute | Opus and GPT-5.5 review the diff **in parallel** | Ensemble, Async |
| Converge | strong-model combiner merges into one review (dedup, rank) | Ensemble, Recipe |
| Land | `gh` posts the combined review — summary + inline replies | Forge verbs |
| Report | "Posted a combined review on PR 27 — three blockers, here's the top one." | Async, Control surface |
| Handoff | you read it on GitHub; "push back on finding 2" re-enters at Execute | Forge, Recipe |
| Learn | job record + which engines were used; trace timings | Observability |

**B. "Build the feature in Linear ticket ENG-412 on my personal laptop, raise the PR."**

| Stage | What happens | Primitive |
|---|---|---|
| Intake | WhatsApp from phone; owner → full caps | Control surface, Trust gate |
| Frame | read ENG-412 via Linear (MCP); expand into a build spec | Work item |
| Plan | engine = `codex` (build) then `panel` (review); target = `personal-laptop` | Engine, Target, Ensemble |
| Provision | route to the laptop target; clone/worktree/branch there | Target, Workspace |
| Execute | Codex implements unattended; tests run | Engine, Async |
| Converge | ensemble review loop until clean | Ensemble, Recipe |
| Land | push branch, open PR, link it to ENG-412, move ticket to "In Review" | Forge verbs, Work item |
| Report | "ENG-412 is built on your laptop, PR #88 is open and linked." | Async, Control surface |
| Handoff | open the laptop, `git switch` the branch, carry on | Workspace |
| Learn | persist session_id for `codex resume`; note the laptop target worked | Observability |

The two traces use the *same machine* with different settings on three knobs:
**engine, target, recipe**. That is the whole design goal.

---

## Part 3 — The primitives

Each primitive: its role in the cycle, an interface sketch (illustrative, not
final), what exists today (with file refs), the gap, and open questions. Status:
✅ have · 🟡 partial · ❌ missing.

### P1 · Engine — *who does the work* 🟡
Role: stages 3, 5. A uniform interface over anything that performs a task: a
**gateway model** (`opus`, `gpt-5.5`) or a **CLI agent** (`codex`, `claude`,
`factory`, `cursor`). BYO-subscription: an engine wraps whatever credential/CLI
is logged in on its target.

```python
class Engine(Protocol):
    name: str                      # "opus", "codex", "claude", ...
    kind: Literal["model", "cli"]
    async def run(self, task: Task, ws: Workspace) -> EngineResult: ...
```

Today: gateway is already model-as-a-parameter — `GatewayClient.complete(
messages, model="…")` (`src/jarvis/brain/gateway_client/__init__.py:33-45`); the worker
shells codex/claude via `code_argv()` (`src/jarvis/worker/actions.py:138-143`). **Gap:** no
common interface; only codex+claude wired; no Factory/Cursor; model routes are
just `fast`/`strong` (`config.py:48`). **Open:** how does a CLI engine stream
progress vs a model engine returning once? Per-engine timeout/cost ceilings?
Where do engine credentials live per target?

### P2 · Target — *where it runs, **and what it can do*** 🟡
Role: stages 3, 4, 9. A named execution backend, reached over the same HTTP
boundary the worker already uses — generalised to a registry so routing ("on my
personal laptop") is a tag. Crucially, **a target is not just a host; it carries a
capability profile** — the skills of that machine — which decides what work can be
*routed* there (stage 3) and how the app can be *exercised* there (P13).

```python
@dataclass
class Target:
    name: str                      # "worker", "personal-laptop", "hive", ...
    base_url: str                  # http(s) over the network boundary
    # MACHINE RESOURCES only — DISCOVERED by a probe ("doctor"), not authority:
    capabilities: set[str]         # {"xcode", "ios-sim", "browser", "docker",
                                   #  "node", "swift", "python", "gpu", ...}
    transport: Literal["push", "pull"]   # always-on vs sleeps/disconnects
```

**Invariant I3 — resources are not authority.** `capabilities` answers only *"can
this machine run that?"*. It must NOT contain credentials or write authority —
*which* GitHub principal `gh` is logged in as, *which* signing identity is present,
*whether this request may push*. A laptop can have `gh` installed yet be
authenticated as the wrong person, or hold a signing cert that this request must
not use. Those are **principal-bound** and resolved by the trust gate (P9) per
request, never inferred from a machine probe.

A task declares what it *needs* (`requires={"xcode","ios-sim"}`); the planner
routes to a target whose `capabilities` satisfy it, or reports *"nothing I can
reach can build a Swift app"*. The profile is **probed**, the way
`jarvis worker --doctor` already inspects peekaboo/GUI readiness — extended to
"is there a browser / Xcode / docker here?". *Credential presence* may also be
probed, but is only ever a routing *hint*; the authority to use it still comes
from P9.

Today: the worker daemon is exactly one such target (`src/jarvis/worker/server.py`), reached
over HTTP with a bearer token, and its `/health` already reports a few
capabilities (browser enabled, GUI provider configured — `src/jarvis/worker/server.py:272`);
the dormant `src/jarvis/remote/` lane is a second, unfinished one (`src/jarvis/remote/client.py`). Phase
2 already plans Tailscale hostnames. **Gap:** no registry, no routing, no
capability profile/probe, one target only. **Open:** push vs pull (pull survives a
laptop asleep/off; needed for serverless); how a result from a transient box gets
home; how a target's repo set / git identity / credentials / toolchains are
provisioned; how fresh the capability probe must be (boot-time vs per-job).

### P3 · Ensemble (panel) — *many engines, one answer* ❌ — the differentiator
Role: stages 5, 6. Fan one task to N engines in parallel, then synthesise. This
is the bit *neither OpenClaw nor Hermes has first-class*, and it's small over the
gateway we already have.

```python
# READ-ONLY panel (e.g. multi-model review of a diff) — may share one ws:
async def review_panel(task, engines, *, synth, ws) -> Result:
    drafts = await asyncio.gather(*(e.run(task, ws) for e in engines))  # no writes
    return await synth.run(combine_prompt(task, drafts), ws)

# WRITING panel (CLI engines that edit files) — each engine its OWN worktree,
# then an explicit converge step picks/merges. NEVER N writers on one tree (I2):
async def build_panel(task, engines, *, converge, base) -> Result:
    branches = await asyncio.gather(*(e.run(task, ws=worktree_of(base, e)) for e in engines))
    return await converge(branches)            # select-best or merge — explicit
```

**Invariant I2** governs this: concurrent engines may share a workspace *only*
when read-only. Writing engines are isolated per worktree/branch and convergence
must be an explicit selection/merge — never a race.

Today: nothing — one model per call. **Gap:** the whole primitive. **Open:**
synthesis as model call vs deterministic merge vs vote; for the writing panel,
select-best vs three-way merge; ensemble members blind (independent, decorrelates
error) vs debate (sees others' drafts); how disagreements surface to the user.

### P4 · Workspace — *isolation* ✅ (git) · ❌ (non-git — P0 bug)
Role: stages 4, 9. Clone, worktree, branch, scratch dir; the branch is the
handoff unit. Today: **for a git repo, isolation is strong and done** — a fresh
worktree on a new branch off HEAD, never the live checkout (`prepare_worktree`,
`clone_repo`, `resolve_repo`, `cleanup_job` — `src/jarvis/worker/actions.py:169-213`).

⚠️ **P0 — non-git inputs are unsafe today (violates invariant I1).** A non-git
`repo` is run **in place** (`prepare_worktree` returns the dir as-is —
`src/jarvis/worker/actions.py:191-192`), the job's `cwd` is set to that real
directory (`src/jarvis/worker/server.py:200`), and on cleanup the `repo and branch
and cwd` guard fails (`branch` is `None`) so it falls through to
`shutil.rmtree(cwd)` (`src/jarvis/worker/actions.py:206-211`) — **deleting the
user's own directory**. Fix (the invariant): a non-git input must be **copied into
worker-owned scratch** (or refused), the job runs on the copy, and cleanup may
only ever remove worker-owned paths. **Open (minor):** worktrees on a *remote*
target — same code, but who cleans up; cross-target caching of clones.

### P5 · Forge verbs — *land the work* 🟡
Role: stages 2, 7. First-class VCS/forge actions: branch, commit, push, **open
PR, comment, review, inline line-comment, link to a work item**. All `gh`/`git`
on a target.

```python
forge.fetch_pr(repo, n) / forge.diff(...) / forge.push(branch)
forge.open_pr(...) / forge.comment(...) / forge.review(summary, inline=[...])
```

**Invariant I4 — public writes are gated and draft-by-default.** Forge verbs split
by blast radius: *private/local* (worktree commit, local branch) are autonomous;
*public* (push a shared branch, open a PR, post a comment/review) require the
`forge.write.public` capability **and** a draft-then-confirm step by default.
Autonomous public writes are opt-in per (principal × repo), set ahead of time —
the north-star "write the replies in the PR" is exactly such a pre-granted
exception, not the default path.

Today: `gh` is present and used for clone (`src/jarvis/worker/actions.py:176`); a
coding engine *can* git from inside a job, but there are **no first-class verbs**
for push/PR/review/comment, and **no `forge.write.public` gate** — so I4 is not yet
enforceable. **Gap:** the verb set + the split `forge.write.local` /
`forge.write.public` capabilities + the draft/confirm flow. **Open:**
inline review comments need file+line anchoring against the diff — done by the
synth step or a dedicated tool? Autonomous-write vs draft-then-confirm (a
trust-gate policy, see P9).

### P6 · Work item — *the durable spec* ❌
Role: stages 2, 7, 9. The unit you *decide from* and anchor a PR to — GitHub
issue, **Linear** ticket, Jira. OpenClaw's "issue as durable spec" pattern.

Today: jobs are ephemeral (`src/jarvis/worker/jobs.py`); no spec/ticket abstraction. Linear
is reachable only as raw MCP tools. **Gap:** a `WorkItem` abstraction + tracker
connectors (GitHub issues native via `gh`; Linear/Jira via the existing MCP
client). **Open:** does Jarvis *create* tickets or only consume them? How does a
ticket map to a job and back (status sync: building → in-review)? One canonical
tracker or many?

### P7 · Async lifecycle — *on it → detached → report back* 🟡
Role: stages 5, 8. Say "on it" now, run detached with the asker's caps, track,
report the outcome proactively. Today: the *dispatch + report* half is strong —
`BackgroundRunner` (`src/jarvis/brain/background.py`) + `proactive.py`; and the
*worker* persists its jobs to disk and reloads them after a restart
(`src/jarvis/worker/jobs.py`). **But recovery across the brain/worker split is
not there**, and for unattended engineering work it's core, not polish:
- The brain's `BackgroundRunner` keeps jobs **in memory only**
  (`src/jarvis/brain/background.py:64`) — a brain restart loses the job and its
  pending proactive report; the work is orphaned.
- There are **two job models** (brain in-memory + worker on-disk) with **no shared
  run id**, so a cross-target run can't be tracked, resumed, or reported as one
  thing. **Gap:** a single persisted cross-target run identity + brain-restart
  recovery of in-flight jobs. **Open:** coarse progress updates mid-job (push vs
  pull — ties to P11).

### P8 · Recipe / orchestration — *the programs* 🟡
Role: stages 3, 6. Compose engines/targets/forge into named flows: ensemble
review, issue→PR loop, plan-then-build. Today: skills exist as markdown recipes
(`src/jarvis/brain/skills.py`) but run a **single strong model** in a tool loop
(`skills.py:99`) — no ensemble, no engine/target awareness. **Gap:** recipes
that can drive P1–P6. **Open:** are recipes still markdown (self-authorable, the
current strength) or code for the complex ones? How does a recipe express "fan
out then converge" declaratively?

### P9 · Trust gate — *no limits, safely* ✅
Role: all stages. Deny-by-default per (device×user); "no limits" = the owner's
full-caps profile on a trusted device, gate intact for everyone else. Today:
strong — `src/jarvis/brain/capabilities.py`, per-device profile front-matter. **Gap (small):**
an owner full-caps profile + new caps (`panel.run`, `forge.write`, `target.*`,
`workitem.*`). **Open:** is forge-*write* always allowed for the owner, or
draft-then-confirm? Per-target ceilings (laptop = full, a shared box = read-only)?

### P10 · Control surface — *command from anywhere* ✅
Role: stages 1, 8. A channel on a device: a voice intercom (Mac, Pi), WhatsApp, a
text console — each a different surface, the device carrying the trust tier (P9).
Today: strong — `src/jarvis/intercom/`, `src/jarvis/connectors/whatsapp/`,
`src/jarvis/connectors/text.py`. No gap for this capability.

### P11 · Observability — *trust & recovery* 🟡
Role: stages 8, 10. "What's running", logs, traces, **resume** handles. Today:
jobs persist with `session_id` captured for `codex resume` (`jobs.py:42,135`);
`jarvis traces`/`jobs` exist. **Gap:** a unified cross-target job/run view;
surfacing resume by voice. **Open:** how much detail reports back by voice vs
stays queryable on demand.

### P12 · Verification gate — *the inner loop's exit condition* ❌ — the part OpenClaw nails
Role: stages 5→6→7 (the inner loop). The component that decides **done vs
iterate**, and thereby makes autonomous work safe to land. It runs the checks and
evaluates the exit predicate; the recipe (P8) loops on its verdict.

```python
@dataclass
class Verdict:
    passed: bool                  # land if True
    tests_ok: bool
    findings: list[Finding]       # each: accepted? actionable? where?
    criteria_met: bool            # vs the WorkItem's acceptance criteria
    iteration: int                # bounded by max_iterations
    escalate: bool                # bound hit / genuinely stuck → hand to human

async def verify(ws, item, *, review: Engine) -> Verdict: ...
```

Today: nothing first-class — a coding engine may run tests ad hoc, but there is
no explicit gate, no "no accepted actionable findings" predicate, no bound, no
escalation. This is the gap the inner-loop section above describes. **Gap:** the
gate + its predicate + the bound/escalation policy. **Open:** what defines
"accepted actionable" (model judgement vs rules)? Where do acceptance criteria
come from — the work item (P6), or inferred? Is the reviewer in the gate the same
ensemble as P3, or a cheaper dedicated checker?

### P13 · Acceptance harness — *exercise the real app* ❌ — the evidence the gate judges
Role: stage 5/6 (feeds P12). Where P12 *decides*, P13 *gathers the evidence by
driving the actual application*. It is a **family of harnesses chosen by app-type ×
the target's capability profile (P2)** — the floor is unit tests; the bar is the
running app observably doing the new thing.

```python
class Harness(Protocol):
    requires: set[str]             # capabilities the target must have (P2)
    async def exercise(self, ws, scenario) -> Evidence: ...
    # Evidence = logs + screenshots/UI-snapshot + HTTP responses + exit codes

HARNESSES = {
  "webapp":  BrowserHarness,   # boot + drive in a browser; assert DOM/console/shots
  "ios":     SimulatorHarness, # build+run on a simulator; UI snapshot / XCUITest
  "service": ServiceHarness,   # boot the service; hit endpoints; assert responses
  "cli":     ProcessHarness,   # run commands; assert stdout/exit
  "library": TestHarness,      # unit/integration only (no app to drive)
}
# selection: detect app-type from the repo, then pick the harness whose
# `requires` ⊆ target.capabilities — else the work can't be verified *here*.
```

Today: the **actuators largely exist or are reachable** — the browser lane
(`src/jarvis/browser/`, CDP/nodriver) can drive a webapp; `control_mac`/peekaboo can drive any
Mac GUI; the worker shell can boot a service and curl it; Apple-platform
build/run/UI-snapshot tooling is reachable on a Mac target. **What's missing is the
concept layer:** app-type detection, the harness selection by capability, the
`Evidence` contract, and wiring evidence into the gate. **Open:** how is app-type
detected (manifest sniff — `package.json`/`*.xcodeproj`/`Cargo.toml` — vs declared
in repo config)? For a *UI* change, what is "observably works" — a golden-screenshot
diff, a model-judged visual check, or an assertion script? Who writes the
exercise scenario — inferred from the work item, or a stored per-repo smoke script?
When no reachable target can run the harness, is "unverifiable here" a hard stop or
a route-elsewhere?

---

## Part 4 — How the bits depend on each other

Build order falls out of the dependency graph, but the *doc's* job is the whole
cycle, not the build. The dependencies:

(P# = primitive; "stage N" = a step of the Part 1 cycle. They are different axes —
a primitive serves one or more stages. The graph below relates primitives and the
few stage dependencies they feed.)

```
P9 trust gate ─── governs ──> everything
P10 surfaces ──── feed ─────> stage 1 Intake (builds the RequestContext) + stage 8 Report

P1 Engine ──┬─> P3 Ensemble        (panel = many engines)
            └─> P8 Recipe          (a recipe picks engines)
P2 Target ──┬─> P4 Workspace       (isolate on the chosen target)
            └─> P8 Recipe          (a recipe picks a target)
P4 Workspace ─> P5 Forge           (land from the worktree)
P2 Target ──(capability profile)──> stage 3 Plan routing (match work to a machine)
                                └──> P13 Harness          (what can be exercised here)
P6 Work item ─> P12 Verify         (acceptance criteria come from the item)
P3 Ensemble ──> P12 Verify         (the review in the gate can be an ensemble)
P13 Harness ──> P12 Verify         (evidence: app exercised → the gate judges it)
P12 Verify ───> P8 Recipe          (recipe loops on the verdict: done vs iterate)
P1+P2+P5+P6 ──> P8 Recipe ──> the inner loop (P12←P13) ──> the cycle ──> P7 Async ──> P8 (iterate)
P11 observes everything; P10 reports it.
```

Foundations everything rides on: **P1 Engine** and **P2 Target** (now carrying a
*capability profile*, so routing and verification both depend on what a machine
can do). The differentiator: **P3 Ensemble** (needs P1). The trust-makers: **P12
Verification gate** + **P13 Acceptance harness** — the inner loop OpenClaw nails,
deepened so it exercises the *real app* (needs P2's profile, P3, P6, P8). The
domain reach: **P5 Forge** + **P6 Work item**. Jarvis already owns the expensive,
boring half — P9, P10, P4-for-git, the dispatch/report half of P7, most of P11 —
and *already has the harness actuators* (browser lane, GUI control). But three of
those carry safety/recovery debt the invariants flag: P4 non-git isolation (I1,
P0), P7 cross-target recovery, and the not-yet-enforceable forge gate (I4).

---

## Part 5 — Cross-cutting design tensions (to resolve together)

- **Push vs pull targets.** Push is simple and matches the current worker; pull
  (target leases work off a queue) survives laptops that sleep/disconnect and is
  the only sane model for serverless. Probably: push for always-on targets, pull
  for transient ones — but that splits the Target interface.
- *(Autonomy vs confirmation at Land is no longer an open tension — it's settled
  as invariant **I4**: public writes are gated + draft-by-default, autonomy is
  pre-granted per principal×repo. What remains is the **UX** of the confirm step,
  not whether it exists.)*
- **Ensemble: blind vs debate.** Independent drafts are simpler and decorrelate
  errors; a debate round can catch more but costs latency and can collapse to
  groupthink. Likely a recipe-level choice, not a fixed rule.
- **Recipes: markdown vs code.** Markdown keeps self-authoring (a real Jarvis
  strength); the fan-out/converge shape may not express cleanly in prose. Maybe a
  small declarative recipe schema sits between.
- **One job model (now core, not tidy-up).** Two job stores today (brain
  in-memory `BackgroundRunner`, worker on-disk `JobManager`) with no shared id —
  this is the P7 recovery gap, a prerequisite for trustworthy unattended runs, not
  a cosmetic merge.
- **Verification depth vs reach.** The deepest gate (boot + drive the real app)
  is also the most capability-hungry, so it's only available where the machine can
  run that app-type. A change might be *buildable* on one target but only
  *verifiable* on another (e.g. CI builds anywhere, but the iOS UI check needs a
  Mac with a simulator). Options: refuse to land unverified, land with a
  "verified: build-only" caveat, or split execute-here / verify-there across two
  targets. This is the practical edge of "the skills of the machine matter".
- **Who writes the exercise scenario.** Driving the app needs *what to do* — a
  login flow, a tap sequence, an endpoint to hit. Inferred from the work item each
  run (flexible, flaky) vs a stored per-repo smoke script (stable, maintenance).
  Probably both: a stored script when present, inference to bootstrap one.

---

## Part 6 — Relationship to OpenClaw & Hermes

- **OpenClaw** contributes the **inner convergence loop** (P12: implement→test→
  review→fix, *iterate until no accepted actionable findings*, bounded, then
  land) — the part it nails and the part that makes autonomous work trustworthy —
  plus the surrounding **issue→PR discipline** (P6→P5→P8: durable issue-as-spec,
  mandatory isolated checkout, one report message) and the **BYO-any-CLI** stance
  (P1: shell out to codex/claude/opencode). We adopt all of it.
- **Hermes** contributes **target-as-a-knob** (P2: local/Docker/SSH/Modal/Daytona
  the same agent, different backend) and **model-agnosticism** (P1). We adopt the
  target registry idea on our existing HTTP boundary.
- **Ours alone:** **P3 Ensemble** — first-class multi-engine fan-out and
  synthesis ("Opus *and* GPT-5.5, combine") — which neither has, and which sits
  naturally on the LiteLLM gateway Jarvis already routes through. And **P13
  Acceptance harness** — verification that *exercises the running app* (browser
  for a webapp, simulator for Swift, boot-and-probe for a service), selected by
  app-type × the target's capability profile. OpenClaw's loop stops at
  test/review; ours closes the feedback loop on the real app, gated by the skills
  of the machine the code lands on.

---

## Settled as invariants (no longer open)

- **I1** Isolation absolute; non-git inputs copied to worker-owned scratch or
  refused; cleanup deletes only worker-owned paths. → First implementation lives
  in `src/jarvis/worker/actions.py` and is protected by worker tests.
- **I2** Panels read-only on a shared workspace; writing engines isolated
  per-worktree with explicit converge. (P3)
- **I3** Machine resources (probed) ≠ principal-bound authority (trust gate). (P2/P9)
- **I4** Public forge writes gated (`forge.write.public`) + draft-by-default;
  autonomy pre-granted per principal×repo. (P5/P9)

## Checkable implementation stages

- [x] **P0 safety prerequisite:** non-git worker jobs copy into worker-owned
      scratch; cleanup refuses non-worker-owned paths.
- [x] **Run graph foundation:** local `run.json` + append-only `events.jsonl`
      under private orchestration workspace; CLI can create/list/show runs and
      sync linked worker job status back into the graph.
- [x] **Worker registry:** named `worker_id` profiles with public-safe status
      output and optional probing.
- [x] **Engine selection foundation:** workers advertise default/supported
      coding engines; WorkCommands and ExecutionEnvelopes carry explicit engine
      selection while preserving `codex` as the default.
- [x] **GitHub work source first:** read-only issue list and PR comment/review
      inspection via the `gh` boundary.
- [x] **WorkCommand intent:** initial deterministic mapper for check/get/fix/
      resume/blocked phrases; LLMs can emit the same structure later.
- [x] **ExecutionEnvelope:** selected work item -> envelope -> existing worker
      `code` job; run graph links the worker job.
- [x] **Linear adapter:** read/list/next plus claim/comment/link methods through
      Linear GraphQL, guarded by `LINEAR_API_KEY`.
- [x] **Time scheduler:** durable scheduled structured WorkCommands with
      daily/weekly-style weekday selection and deterministic tick checks.
- [x] **Campaign primitive:** parent run creates bounded child runs and stops
      cleanly on empty queues.

## Dogfood follow-up notes

These came out of the first local orchestration smoke pass. They should be
handled at the stage where they belong rather than widening the current slice:

- [x] **Foundation hardening:** default worker workspaces outside the Jarvis repo;
      reject worker workspaces inside any git checkout; use `jarvis-` names for
      orchestration worktrees/branches; surface worker dispatch error bodies.
- [x] **Run graph observability:** `runs --sync` refreshes linked worker job
      status, branch, cwd, and `codex resume` session id from the worker daemon.
- [x] **Work-source UX:** `work check` and `work pr-comments` should summarize
      useful next actions by default, with raw JSON kept behind `--json`.
- [x] **Capability guidance:** when authority is missing, CLI output should show
      the exact local profile/config location to edit and whether it applies to
      the brain profile or named worker profile.
- [ ] **Smoke dispatch command:** add a disposable, explicit orchestration smoke
      command that creates or selects safe test work and cleans up after itself.

## Open questions log (fill as we go)

- [ ] Push vs pull (or both) for the Target interface? (P2)
- [ ] I4 confirm-step UX: how a voice/WhatsApp draft-then-confirm actually feels. (P5)
- [ ] WorkItem: consume-only or also create? Canonical tracker or many? (P6)
- [ ] Ensemble synthesis: model / merge / vote, and blind vs debate? (P3)
- [ ] Recipe representation: markdown, code, or a declarative schema? (P8)
- [ ] Cross-target run identity + brain-restart recovery shape? (P7/P11)
- [ ] Engine progress protocol: streaming CLI vs one-shot model? (P1)
- [ ] Verification gate: what defines "accepted actionable"; where do acceptance
      criteria come from; is the gate's reviewer the P3 ensemble or a cheaper
      checker; tests discovered vs declared? (P12)
- [ ] Inner-loop bound: max_iterations + cost/wall-clock budget values, and the
      escalation message shape when the bound is hit? (P12/P8)
- [ ] Target capability profile: probed vs declared; how fresh; what's in the
      vocabulary (toolchains, devices, credentials, resources)? (P2)
- [ ] Acceptance harness: how is app-type detected; what is "observably works"
      for a UI change (golden-shot / model-judged / scripted); stored smoke
      script vs inferred scenario? (P13)
- [ ] Verification depth vs reach: when a target can build but not exercise an
      app-type, refuse / caveat / execute-here-verify-there? (P2/P12/P13)
- [ ] Is the 10-stage cycle the right cut, or are stages missing/merged?

---

## Appendix — source links (OpenClaw & Hermes)

Where the prior-art in this doc comes from. ✓ = fetched/read during research;
~ = seen in search results only, not independently verified. Community links are
third-party write-ups — useful orientation, accuracy not guaranteed.

### OpenClaw — official
- ✓ Docs home — https://docs.openclaw.ai/
- ✓ Concept: agent workspace (AGENTS.md/SOUL.md/MEMORY.md model) —
  https://docs.openclaw.ai/concepts/agent-workspace
- ✓ GitHub repo — https://github.com/openclaw/openclaw
- ✓ **coding-agent skill** (the issue→PR loop; codex/claude/opencode as
  background workers; isolated-checkout rule) —
  https://github.com/openclaw/openclaw/blob/main/skills/coding-agent/SKILL.md
- ✓ Repo AGENTS.md — https://github.com/openclaw/openclaw/blob/main/AGENTS.md

### OpenClaw — community / ecosystem
- ~ claw-orchestrator — run Claude Code, Codex, Gemini, Cursor Agent, custom CLIs
  as one runtime (relevant to P1 Engine BYO-CLI) —
  https://github.com/Enderfga/claw-orchestrator
- ~ awesome-openclaw-skills · coding-agents-and-ides —
  https://github.com/VoltAgent/awesome-openclaw-skills/blob/main/categories/coding-agents-and-ides.md
- ~ openclaw-code-agent — https://github.com/goldmar/openclaw-code-agent
- ~ openclaw-workspace (Claude Code skill for the workspace files) —
  https://github.com/win4r/openclaw-workspace
- ~ AgentHandover (observe-and-teach for OpenClaw/Claude/Codex) —
  https://github.com/sandroandric/AgentHandover
- ~ coding-agent skill mirror — https://playbooks.com/skills/openclaw/skills/coding-agent

### Hermes (Nous Research) — official
- ✓ Docs home — https://hermes-agent.nousresearch.com/docs/
- ✓ Product landing — https://hermes-agent.nousresearch.com/
- ✓ GitHub repo — https://github.com/NousResearch/hermes-agent
- ✓ **Tools & toolsets** (terminal/process/patch/execute_code, browser_*,
  cronjob, delegate_task subagents, MCP) —
  https://hermes-agent.nousresearch.com/docs/user-guide/features/tools
- ✓ Configuration (6 terminal backends: local/Docker/SSH/Singularity/Modal/Daytona) —
  https://hermes-agent.nousresearch.com/docs/user-guide/configuration/
- ~ Configuration source on GitHub —
  https://github.com/NousResearch/hermes-agent/blob/main/website/docs/user-guide/configuration.md

### Hermes — community
- ~ hermes-agent-docs (comprehensive third-party docs, v0.2.0) —
  https://github.com/mudrii/hermes-agent-docs
- ~ Hermes vs OpenClaw vs GoClaw comparison (dev.to) —
  https://dev.to/truongpx396/hermes-agent-the-self-improving-agent-framework-and-how-it-compares-to-openclaw-goclaw-22mc
- ~ Hermes Agent multi-agent setup guide (codersera) —
  https://codersera.com/blog/hermes-agent-guide-to-multi-agent-ai-setup/
- ~ Hermes Agent overview (dsebastien.net) — https://www.dsebastien.net/hermes-agent/

> Note: some figures quoted by page-summarisers during research (GitHub star/commit
> counts) looked inflated and were **not** trusted; the architecture above relies on
> the qualitative, cross-corroborated content of the ✓ sources, not those metrics.
