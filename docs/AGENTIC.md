# AGENTIC.md — the agentic engineering cycle

**Status: design / decomposition. Nothing here is built yet.** This is the shared
map we argue over and fill in. It describes the *whole* capability — commanding
real engineering work by voice/text and having it done across machines and
agents — as a **single closed cycle**, then pulls that cycle apart into the
primitives that power it. The five currently-missing primitives are called out,
but always *in their place in the loop*, never as a standalone backlog: the point
is the complete cycle, including them.

> The north-star use: *"review PR 27 on `<repo>` with Opus and GPT-5.5, combine,
> and write the replies in the PR"* — or *"take the Linear ticket, build the
> feature on my personal laptop, raise the PR, tell me when it's up"* — said out
> loud to Jarvis on the TV, run unattended, picked up locally later. These are
> not features; they are *programs* run on the machine this doc specifies.

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

1. **Intake.** A request lands on a *control surface* (TV voice, WhatsApp, text
   console). Identity and trust are resolved → a `RequestContext` with a
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
   workspace** — clone the repo if missing, cut a worktree on a fresh branch.
   Never the user's live checkout.
5. **Execute.** Run the engine(s) on the target in the workspace. One engine, or
   **an ensemble in parallel** (panel). Long-running, async, tracked as a job.
6. **Converge.** If an ensemble ran, **synthesise** the outputs. Run the
   **self-review loop** — review/test until there are no accepted actionable
   findings (OpenClaw's issue→PR discipline).
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
- **Isolated.** Every iteration runs in the P4 worktree, never the user's
  checkout (OpenClaw's mandatory-isolated-checkout rule).
- **One report, at the end.** The loop runs detached (P7) and emits exactly one
  completion/failure message with the result + a resume handle (P11) — it does
  not chatter per iteration (unless it needs to ask a question it genuinely
  cannot answer alone).

This nested loop sits *inside* the outer cycle. The **outer** loop is
human-driven (stage 9 handoff: "address the review comments" re-enters at stage
5); the **inner** loop is autonomous (converge before landing). Both must exist;
conflating them is what the first draft of this doc got wrong.

---

## Part 2 — Two worked traces

**A. "Review PR 27 on `acme/api` with Opus and GPT-5.5, combine, write the replies."**

| Stage | What happens | Primitive |
|---|---|---|
| Intake | TV voice; owner on trusted device → full caps | Control surface, Trust gate |
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
messages, model="…")` (`brain/gateway_client/__init__.py:33-45`); the worker
shells codex/claude via `code_argv()` (`worker/actions.py:138-143`). **Gap:** no
common interface; only codex+claude wired; no Factory/Cursor; model routes are
just `fast`/`strong` (`config.py:48`). **Open:** how does a CLI engine stream
progress vs a model engine returning once? Per-engine timeout/cost ceilings?
Where do engine credentials live per target?

### P2 · Target — *where it runs* 🟡
Role: stages 3, 4, 9. A named execution backend. The same HTTP boundary the
worker already uses, generalised to a registry so routing ("on my personal
laptop") is a tag.

```python
# config-driven registry
targets = {
  "worker":         http://localhost:8782,     # this Mac (default)
  "personal-laptop": http://laptop.tailnet:8782,
  "hive":           http://hive.tailnet:8782,   # Phase 2 heavy tier
  "serverless":     <Modal/Daytona/Claude-Managed-Agents adapter>,
}
```

Today: the worker daemon is exactly one such target (`worker/server.py`),
reached over HTTP with a bearer token; the dormant `remote/` lane is a second,
unfinished one (`remote/client.py`). Phase 2 already plans Tailscale hostnames.
**Gap:** no registry, no routing, one target only. **Open:** push (brain calls
target) vs pull (target polls a queue) — pull survives a laptop being asleep/off
better; how does a result from a transient serverless box get home; how is a
target's repo set / git identity / credentials provisioned?

### P3 · Ensemble (panel) — *many engines, one answer* ❌ — the differentiator
Role: stages 5, 6. Fan one task to N engines in parallel, then synthesise. This
is the bit *neither OpenClaw nor Hermes has first-class*, and it's small over the
gateway we already have.

```python
async def panel(task, engines: list[Engine], *, synth: Engine) -> Result:
    drafts = await asyncio.gather(*(e.run(task, ws) for e in engines))
    return await synth.run(combine_prompt(task, drafts), ws)
```

Today: nothing — one model per call. **Gap:** the whole primitive. **Open:** is
synthesis a model call, a deterministic merge, or a vote? Do ensemble members
see each other's drafts (debate) or stay blind (independent)? How are
disagreements surfaced to the user vs resolved silently?

### P4 · Workspace — *isolation* ✅
Role: stages 4, 9. Clone, worktree, branch, scratch dir; the branch is the
handoff unit. Today: strong and done — `prepare_worktree`, `clone_repo`,
`resolve_repo`, `cleanup_job` (`worker/actions.py:169-213`). **Open (minor):**
worktrees on a *remote* target — same code, but who cleans up; cross-target
caching of clones.

### P5 · Forge verbs — *land the work* 🟡
Role: stages 2, 7. First-class VCS/forge actions: branch, commit, push, **open
PR, comment, review, inline line-comment, link to a work item**. All `gh`/`git`
on a target.

```python
forge.fetch_pr(repo, n) / forge.diff(...) / forge.push(branch)
forge.open_pr(...) / forge.comment(...) / forge.review(summary, inline=[...])
```

Today: `gh` is present and used for clone (`actions.py:176`); a coding engine
*can* git from inside a job, but there are **no first-class verbs** for
push/PR/review/comment. **Gap:** the verb set + a `forge.*` capability. **Open:**
inline review comments need file+line anchoring against the diff — done by the
synth step or a dedicated tool? Autonomous-write vs draft-then-confirm (a
trust-gate policy, see P9).

### P6 · Work item — *the durable spec* ❌
Role: stages 2, 7, 9. The unit you *decide from* and anchor a PR to — GitHub
issue, **Linear** ticket, Jira. OpenClaw's "issue as durable spec" pattern.

Today: jobs are ephemeral (`worker/jobs.py`); no spec/ticket abstraction. Linear
is reachable only as raw MCP tools. **Gap:** a `WorkItem` abstraction + tracker
connectors (GitHub issues native via `gh`; Linear/Jira via the existing MCP
client). **Open:** does Jarvis *create* tickets or only consume them? How does a
ticket map to a job and back (status sync: building → in-review)? One canonical
tracker or many?

### P7 · Async lifecycle — *on it → detached → report back* ✅
Role: stages 5, 8. Say "on it" now, run detached with the asker's caps, track,
report the outcome proactively. Today: strong — `BackgroundRunner`
(`brain/background.py`) + `JobManager` persistence (`worker/jobs.py`) +
`proactive.py`. **Open:** unify the brain's `BackgroundRunner` jobs with the
worker's `JobManager` jobs into one job view across targets; progress updates
mid-job (not just final), for very long runs.

### P8 · Recipe / orchestration — *the programs* 🟡
Role: stages 3, 6. Compose engines/targets/forge into named flows: ensemble
review, issue→PR loop, plan-then-build. Today: skills exist as markdown recipes
(`brain/skills.py`) but run a **single strong model** in a tool loop
(`skills.py:99`) — no ensemble, no engine/target awareness. **Gap:** recipes
that can drive P1–P6. **Open:** are recipes still markdown (self-authorable, the
current strength) or code for the complex ones? How does a recipe express "fan
out then converge" declaratively?

### P9 · Trust gate — *no limits, safely* ✅
Role: all stages. Deny-by-default per (device×user); "no limits" = the owner's
full-caps profile on a trusted device, gate intact for everyone else. Today:
strong — `brain/capabilities.py`, per-device profile front-matter. **Gap (small):**
an owner full-caps profile + new caps (`panel.run`, `forge.write`, `target.*`,
`workitem.*`). **Open:** is forge-*write* always allowed for the owner, or
draft-then-confirm? Per-target ceilings (laptop = full, a shared box = read-only)?

### P10 · Control surface — *command from anywhere* ✅
Role: stages 1, 8. Voice/TV, WhatsApp, text console. Today: strong — `intercom/`,
`connectors/whatsapp/`, `connectors/text.py`. No gap for this capability.

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
ensemble as P3, or a cheaper dedicated checker? Tests: discovered (`make test`,
CI config) or declared per repo?

---

## Part 4 — How the bits depend on each other

Build order falls out of the dependency graph, but the *doc's* job is the whole
cycle, not the build. The dependencies:

```
P9 trust gate ─── governs ──> everything
P10 surfaces ──── feed ─────> P1 intake/report

P1 Engine ──┬─> P3 Ensemble        (panel = many engines)
            └─> P8 Recipe          (a recipe picks engines)
P2 Target ──┬─> P4 Workspace       (isolate on the chosen target)
            └─> P8 Recipe          (a recipe picks a target)
P4 Workspace ─> P5 Forge           (land from the worktree)
P6 Work item ─> P12 Verify         (acceptance criteria come from the item)
P3 Ensemble ──> P12 Verify         (the review in the gate can be an ensemble)
P12 Verify ───> P8 Recipe          (recipe loops on the verdict: done vs iterate)
P1+P2+P5+P6 ──> P8 Recipe ──> the inner loop (P12) ──> the cycle ──> P7 Async ──> P8 (iterate)
P11 observes everything; P10 reports it.
```

Foundations everything rides on: **P1 Engine** and **P2 Target**. The
differentiator: **P3 Ensemble** (needs P1). The trust-maker: **P12 Verification
gate** (the inner loop OpenClaw nails — needs P3+P6+P8). The domain reach:
**P5 Forge** + **P6 Work item**. Jarvis already owns P4, P7, P9, P10 and most of
P11 — the expensive, boring half.

---

## Part 5 — Cross-cutting design tensions (to resolve together)

- **Push vs pull targets.** Push is simple and matches the current worker; pull
  (target leases work off a queue) survives laptops that sleep/disconnect and is
  the only sane model for serverless. Probably: push for always-on targets, pull
  for transient ones — but that splits the Target interface.
- **Autonomy vs confirmation at the Land stage.** "No limits" argues for
  autonomous forge writes; a misheard command argues for draft-then-confirm on
  irreversible/public actions. The existing background lane already encodes
  "you've consented by asking" for read/build; *public writes* (PR comments,
  pushes to shared branches) may deserve a separate policy bit.
- **Ensemble: blind vs debate.** Independent drafts are simpler and decorrelate
  errors; a debate round can catch more but costs latency and can collapse to
  groupthink. Likely a recipe-level choice, not a fixed rule.
- **Recipes: markdown vs code.** Markdown keeps self-authoring (a real Jarvis
  strength); the fan-out/converge shape may not express cleanly in prose. Maybe a
  small declarative recipe schema sits between.
- **One job model.** Today there are two (brain `BackgroundRunner`, worker
  `JobManager`). A cross-target cycle wants one job identity that spans them.

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
  naturally on the LiteLLM gateway Jarvis already routes through.

---

## Open questions log (fill as we go)

- [ ] Push vs pull (or both) for the Target interface? (P2)
- [ ] Forge writes: autonomous vs draft-then-confirm policy, and where it's set? (P5/P9)
- [ ] WorkItem: consume-only or also create? Canonical tracker or many? (P6)
- [ ] Ensemble synthesis: model / merge / vote, and blind vs debate? (P3)
- [ ] Recipe representation: markdown, code, or a declarative schema? (P8)
- [ ] Unify the two job models into one cross-target run identity? (P7/P11)
- [ ] Engine progress protocol: streaming CLI vs one-shot model? (P1)
- [ ] Verification gate: what defines "accepted actionable"; where do acceptance
      criteria come from; is the gate's reviewer the P3 ensemble or a cheaper
      checker; tests discovered vs declared? (P12)
- [ ] Inner-loop bound: max_iterations + cost/wall-clock budget values, and the
      escalation message shape when the bound is hit? (P12/P8)
- [ ] Is the 10-stage cycle the right cut, or are stages missing/merged?
```
