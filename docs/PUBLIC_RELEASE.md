# Public Repository Readiness

Jarvis is public. This document records the gates that keep the public repos
safe to install from and safe to publish.

## Current Status

| Area | Status | Required action |
|---|---|---|
| Worker job records | Fixed | Tracked `jarvis-workspace/worker/jobs/*.json` records were removed. Keep the directory ignored. |
| User files | Guarded | Never track `jarvis-workspace/users/*.md`; they contain personal identity bindings. |
| OAuth caches | Guarded | Keep `jarvis-workspace/.mcp-auth/` ignored and verify no cached token files are tracked. |
| Browser profile | Guarded | Keep `jarvis-workspace/browser/` ignored; Chrome profiles contain cookies, history, and login metadata. |
| `.env` | Guarded | `.env.example` must use placeholders only. No copied local secrets. |
| Private Homebrew release | Fixed | `jarvis-app` cask uses the public GitHub release download URL, not the release asset API. |
| Runtime package | Fixed | `jarvis` v0.1.1 is published as a versioned GitHub release tarball and the Homebrew formula uses the public release URL. |
| Docs | Fixed locally | Install-first deployment docs and a static docs preview site are present. |
| GitHub Actions | Fixed | CI and public-readiness workflows run successfully on public repos. |
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

Completed:

- Repos are public.
- Required scans pass through `scripts/verify_public_readiness.sh`.
- The Homebrew app cask uses public release download URLs.
- The runtime formula uses a public versioned tarball URL.
- CI and public-readiness workflows run on GitHub.
- Public install docs exist; private fleet credentials remain local machine
  state only.
