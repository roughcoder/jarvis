# CLAUDE.md

Project guidance for this repo lives in **@AGENTS.md** — read it first
(architecture, the two hard constraints, setup/run, CLI commands, conventions,
and gotchas).

Quick reminders:
- Python is pinned to 3.12 via `uv`; lint with `uv run ruff check src/`.
- All config comes from env via `src/jarvis/config.py`; never hardcode hosts,
  ports, keys, or model names.
- Release notes are generated from commit messages. Put user-facing detail in
  trailers: `Release-note:`, `Env:`, and `Breaking Change:`.
- Honour the two hard constraints in AGENTS.md (network boundary everywhere;
  the hot path never blocks on a memory write).
