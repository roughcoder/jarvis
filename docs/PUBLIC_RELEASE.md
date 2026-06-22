# Public Repository Readiness

Jarvis can become public only after the repository stops containing deployment
state, personal data, private release assumptions, and local-machine artifacts.

## Current Blockers

| Area | Status | Required action |
|---|---|---|
| Worker job records | Fixed | Tracked `jarvis-workspace/worker/jobs/*.json` records were removed. Keep the directory ignored. |
| User files | Guarded | Never track `jarvis-workspace/users/*.md`; they contain personal identity bindings. |
| OAuth caches | Guarded | Keep `jarvis-workspace/.mcp-auth/` ignored and verify no cached token files are tracked. |
| Browser profile | Guarded | Keep `jarvis-workspace/browser/` ignored; Chrome profiles contain cookies, history, and login metadata. |
| `.env` | Guarded | `.env.example` must use placeholders only. No copied local secrets. |
| Private Homebrew release | Fixed | `jarvis-app` cask uses the public GitHub release download URL, not the release asset API. |
| Runtime package | Fixed for beta | `jarvis` formula exists. Runtime release automation now creates versioned tarballs and can update the formula; the formula remains `--HEAD` until the first runtime release is published. |
| Docs | Fixed locally | Install-first deployment docs and a static docs preview site are present. |
| GitHub Actions | External blocker | All workflows currently fail at `startup_failure` before any job starts, including a no-checkout smoke workflow. This points to GitHub account/repo Actions execution rather than workflow content. |
| GitHub Pages | Deferred | Ignore Pages for now. `docs-site/` and a prepared `gh-pages` branch exist, but Pages hosting is not part of the current go-public gate. |

## Required Scans

Run these before changing repository visibility:

```bash
scripts/verify_public_readiness.sh
```

The verifier checks the runtime, sibling `jarvis-apple`, and sibling
`homebrew-infinite-stack` checkouts. Override locations when needed:

```bash
JARVIS_APP_DIR=/path/to/jarvis-apple \
JARVIS_TAP_DIR=/path/to/homebrew-infinite-stack \
scripts/verify_public_readiness.sh
```

Manual scan primitives:

```bash
git status --short
git ls-files | rg '(^|/)(\\.env|.*secret.*|.*token.*|.*key.*|.*pem|.*p12|.*sqlite|.*db|.*jsonl|\\.mcp-auth|browser|worker/jobs)'
git ls-files | xargs rg -l -i '(api[_-]?key|secret|token|password|authorization: bearer|sk-[A-Za-z0-9]|ghp_|github_pat_|BEGIN (RSA|OPENSSH|PRIVATE) KEY)'
```

The second command should only return intentional examples, tests, or docs. The
third command is broad and will produce false positives, but every hit must be
reviewed before publication.

## Files That May Be Public

- Safe docs and architecture notes
- `.env.example` with empty provider keys and example-only local development keys
- launchd/systemd templates without local paths or tokens
- capability profile templates that do not name real people or accounts
- test fixtures with fake tokens only

## Files That Must Stay Private

- `.env`
- `jarvis-workspace/users/*.md`
- `jarvis-workspace/.mcp-auth/**`
- `jarvis-workspace/browser/**`
- `jarvis-workspace/worker/jobs/**`
- `jarvis-workspace/worker/runs/**`
- local memory caches and traces
- real Homebrew/GitHub tokens
- generated release archives before publication

## Visibility Change Checklist

1. Run the required scans and resolve every non-example hit.
2. Rotate any secret that ever appeared in git history or local logs.
3. Confirm the Homebrew cask still uses public release download URLs.
4. Publish the first runtime release so the `jarvis` formula moves from
   `--HEAD` only to a versioned tarball URL.
5. Confirm CI workflow files pass `actionlint`, then fix the GitHub Actions
   account/repo startup issue so remote CI jobs actually run.
6. Publish public install docs and keep private fleet credentials in local machine
   state only.
7. Change repository visibility only after the app and runtime formula
   no longer depend on private GitHub asset access.
