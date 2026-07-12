# Runtime Dogfood Runbook

This runbook defines the inner delivery loop for changes that affect the Jarvis
Cockpit API, orchestration parents, or coding workers:

```text
commit locally -> deploy exact SHA -> test through live Cockpit -> fix -> repeat
       -> open PR -> review through Cockpit -> merge -> publish one release
```

The purpose of the dogfood ring is to remove release latency from development
without replacing production packaging. Homebrew and the release workflow remain
the only production distribution path.

## Boundaries

The ring is deliberately small and reversible:

- Cockpit API and orchestrator worker on the central review host.
- A linked review worker with the required Codex and Claude subscription CLIs.
- `launchd` hosts only.
- No brain voice service, intercom, room device, memory/gateway service,
  WhatsApp connector, Homebrew formula, tag, or GitHub release change.
- No irreversible data or schema migration.

The fleet inventory is private local state. Keep it outside the repository at
`~/.jarvis/dogfood-fleet.json` or pass another untracked path with
`--inventory`. Public examples use placeholders only; never copy hostnames,
addresses, tokens, provider credentials, or local setup history into tracked
files, commits, PRs, screenshots, or reports.

## Deployment Model

`jarvis dogfood` builds from a committed 40-character git SHA. Every host first
prepares the same immutable source archive and role-scoped `uv` environment
under:

```text
~/.jarvis/dogfood/builds/<git-sha>/
```

Only after every host prepares successfully does activation begin. Services
point at a stable launcher under `~/.jarvis/dogfood/bin/`; activation atomically
switches its target and reloads the selected launchd services. Each process
reports:

- runtime version;
- deployment channel (`dogfood` or `production`);
- exact dogfood git SHA.

Activation fails if a configured API or worker probe does not report the
selected identity. Hosts already switched by the same transaction are restored.
The currently released production runtime predates build identity; it is treated
as a valid rollback target only when the selected executable resolves to the
inventory's exact `production_bin`.

## Private Inventory

Each selected role needs exactly one health probe. Token-bearing probes may
target loopback only. The linked Claude worker needs the `worker-claude` extra;
the API role automatically includes its gateway import dependency.

```json
{
  "hosts": [
    {
      "name": "review-brain",
      "ssh": "review-host-alias",
      "roles": ["api", "worker"],
      "workdir": "~/.jarvis",
      "runtime_root": "~/.jarvis/dogfood",
      "production_bin": "/opt/homebrew/bin/jarvis",
      "uv_bin": "/opt/homebrew/bin/uv",
      "python": "3.12",
      "probes": [
        {"role": "api", "url": "http://review-host.private:8790/v1/runtime"},
        {"role": "worker", "url": "http://127.0.0.1:8780/health"}
      ]
    },
    {
      "name": "review-laptop",
      "local": true,
      "roles": ["worker"],
      "extras": ["worker-claude"],
      "probes": [
        {"role": "worker", "url": "http://127.0.0.1:8780/health"}
      ]
    }
  ]
}
```

The command rejects tracked inventories, duplicate hosts, unsupported roles or
extras, missing/duplicate probes, credential-bearing non-loopback probes, and
non-launchd hosts.

## Inner Loop

### 1. Create an isolated candidate

Start from current `origin/main` in a dedicated worktree. Preserve unrelated
user changes in other worktrees.

```bash
git fetch origin
git worktree add ../jarvis-<task> -b codex/<task> origin/main
cd ../jarvis-<task>
```

Implement and run the affected tests. Commit every tracked and untracked source
file needed by the candidate; deploy refuses a dirty worktree because
`git archive` can only include committed files.

### 2. Verify locally

Run focused tests first, followed by the normal runtime gates in proportion to
the change:

```bash
uv run ruff check src/ tests/ scripts/generate_release_notes.py
uv run pytest tests/unit -q
scripts/verify_public_readiness.sh
```

Use a clean current Homebrew-tap checkout when the public-readiness script checks
release manifests. Do not modify or discard a user's dirty tap checkout to make
the gate pass.

### 3. Preview and deploy the exact SHA

```bash
uv run jarvis dogfood deploy HEAD --dry-run
uv run jarvis dogfood deploy HEAD
uv run jarvis dogfood status
```

The dry run must name every intended host and resolve one SHA. After activation,
`status` must report `ok: true`, `channel: dogfood`, and the same full SHA for
every API and worker probe. Do not begin Cockpit testing with a mixed or unknown
ring.

### 4. Prove rollback when deployment machinery changes

Changes to the dogfood command, launcher, service templates, health identity, or
role dependencies require a live rollback drill:

```bash
uv run jarvis dogfood rollback
uv run jarvis dogfood status
uv run jarvis dogfood deploy HEAD
uv run jarvis dogfood status
```

The first status must show the configured production target healthy. The second
must restore the exact candidate SHA on every host. Rollback switches targets
and restarts roles; it never reinstalls Homebrew.

### 5. Test through the live product path

Run Cockpit locally using its normal development command, but keep its Jarvis
connection pointed at the central live API over the private network. Do not
replace this with an API/worker running on the development laptop, and do not
mock the worker providers for acceptance.

For each fixture PR:

1. Open the project PR panel and click **Review**.
2. Use the configured default orchestrator model, overriding it only when the
   review requires a specific parent model.
3. Select exactly two reviewers:
   - `Claude · Claude Opus 4.7`
   - `Codex · GPT-5.5`
4. Start the review and verify that Cockpit creates a code-agent orchestrator,
   not a normal Jarvis/gateway chat.
5. Verify both child review chats appear beneath the parent without reloading.
   Each child must show the parent relationship icon and run on the intended
   review worker using its local subscription CLI authentication.
6. Verify both reviewers inspect the same repository, PR number, and immutable
   head SHA.
7. Verify the parent observes both terminal child results, performs one automatic
   continuation, de-duplicates the findings, and publishes one GitHub review.
8. Inspect the GitHub review:
   - actionable findings are inline on the relevant changed lines;
   - comment titles start with `[P1]`, `[P2]`, or `[P3]`;
   - safe concrete fixes use GitHub suggestion blocks;
   - the body explains impact and evidence;
   - no duplicate comments, orphan sessions, or extra reviews are created.

The priority belongs in each PR comment title. It is not merely a label in the
Cockpit review dialog or parent chat.

### 6. Fix and repeat

When a test fails:

1. Record the smallest non-sensitive symptom and evidence.
2. Fix it on the same local branch.
3. Run focused regression tests.
4. Commit a new candidate SHA.
5. Deploy that SHA to the same ring.
6. Repeat the Cockpit flow from the Review button.

Do not open the runtime PR while the live path is still failing. Do not publish a
temporary release to move code between iterations.

## Acceptance Gate

A candidate is ready for a runtime PR only when all of these are true:

- [ ] Every ring process reports the same dogfood SHA.
- [ ] Rollback is proven if deployment machinery changed.
- [ ] Codex and Claude both authenticate through their local subscription CLIs.
- [ ] The orchestrator is a code-agent session with the configured parent model.
- [ ] Two children start on the intended review worker.
- [ ] Parent-child hierarchy appears live without a page reload.
- [ ] Both children review the same PR head SHA.
- [ ] The parent continues automatically exactly once after both finish.
- [ ] One combined GitHub review is published.
- [ ] Findings are line-aligned with `[P1]`/`[P2]`/`[P3]` titles.
- [ ] Safe fixes use suggestion blocks.
- [ ] No duplicate reviews, duplicate comments, or orphan sessions remain.
- [ ] Both fixture PRs pass the complete flow consecutively.
- [ ] Local lint, unit, and public-readiness gates pass at the final SHA.

## PR and Release Gate

After acceptance:

1. Push the existing dogfood branch and open one Jarvis runtime PR.
2. Review that PR through the same Cockpit two-model flow.
3. Address and resolve actionable review threads on the same branch.
4. Merge only when checks and reviews are clean.
5. Publish one normal runtime release through GitHub Actions.
6. Let the release workflow update Homebrew and then return the ring to the
   released production binary when desired.

Dogfood permission does not grant release permission. Never create tags, GitHub
releases, assets, or Homebrew release updates during the inner loop.

## Troubleshooting

### Preparation fails

- Confirm the worktree is fully clean and the requested ref resolves to a commit.
- Confirm every host has the configured `uv` and Python version.
- If an immutable SHA directory exists without a manifest, treat it as an
  incomplete/concurrent preparation; do not overwrite another live prepare.
- If a role fails to import at startup, fix the public role-extra contract rather
  than installing packages manually into one host.

### Activation or status fails

- Trust process identity from the health response, not only the selected symlink
  or `launchctl` state.
- A dogfood probe with a missing/different SHA is a hard failure.
- The exact configured production binary may lack runtime identity until the
  next release; only that known target receives the legacy compatibility rule.
- launchd plist changes require bootout/bootstrap; kickstart alone does not
  reload changed program arguments.

### The orchestrator stalls before creating children

- Inspect the worker session events for an MCP tool approval elicitation.
- The ephemeral `jarvis_orchestrator` MCP server must use approved tool mode;
  its signed thread/project/tool-scoped grant remains the authorization boundary.
- Unexpected MCP elicitations must fail closed instead of hanging headless.
- Confirm completed `mcpToolCall` items are projected as tool-result events.

### A provider does not authenticate

- Run the provider's local CLI auth/status command as the same service user.
- Claude must use the local Claude subscription/SDK session; Codex must use the
  local Codex subscription session.
- Do not add provider API keys merely to make this review path pass.

### Diagnostic output exposes a credential

Never print launchd environments, full service commands containing credentials,
or unredacted dotenv files. If a token appears in logs or terminal output, stop
copying the output, rotate the token, and verify the replacement without
displaying it.

## Evidence to Retain

For each accepted candidate, retain a concise private record of:

- candidate SHA and status output;
- rollback/redeploy result when required;
- the two fixture PR numbers and head SHAs;
- parent and child session references;
- GitHub review URL;
- screenshots showing live hierarchy and the resulting inline comments;
- local verification commands and pass counts.

Evidence may reference private session IDs locally, but public PR descriptions
must contain only product-level behavior and sanitized verification results.
