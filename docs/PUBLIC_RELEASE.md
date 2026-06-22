# Public Repository Readiness

Jarvis can become public only after the repository stops containing deployment
state, personal data, private release assumptions, and local-machine artifacts.

## Current Blockers

| Area | Status | Required action |
|---|---|---|
| Worker job records | Fixed locally | Tracked `jarvis-workspace/worker/jobs/*.json` records were removed. Keep the directory ignored. |
| User files | Ignored locally | Never track `jarvis-workspace/users/*.md`; they contain personal identity bindings. |
| OAuth caches | Ignored locally | Keep `jarvis-workspace/.mcp-auth/` ignored and verify no cached token files are tracked. |
| Browser profile | Ignored locally | Keep `jarvis-workspace/browser/` ignored; Chrome profiles contain cookies, history, and login metadata. |
| `.env` | Ignored locally | `.env.example` must use placeholders only. No copied local secrets. |
| Private Homebrew release | Open | Convert `jarvis-app` cask from authenticated private asset URL to public release URL once repo visibility changes. |
| Runtime package | Open | Add public `jarvis` formula before claiming no-clone installation. |
| Docs | Open | Replace development-first quickstarts with install-first paths. Keep development instructions separate. |

## Required Scans

Run these before changing repository visibility:

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
3. Convert private GitHub release URLs in the Homebrew cask to public release URLs.
4. Add CI for Python tests, Swift tests, Homebrew style/audit, and docs build.
5. Publish public install docs and keep private fleet credentials in local machine
   state only.
6. Change repository visibility only after the app, runtime formula, and docs site
   no longer depend on private GitHub asset access.
