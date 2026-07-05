# Honcho v3 Cutover Runbook

This runbook is for the later real cutover. Step 9 only builds and proves the
tooling against dev or throwaway v3 workspaces. Do not use this document as
permission to retire the v2 stack.

## Safety Boundary

- The source of truth for deliberately saved personal facts is
  `jarvis-workspace/users/<name>.md`, under `## What Jarvis knows`.
- The cutover imports those facts into a fresh Honcho v3 workspace as explicit
  conclusions on the matching principal peer.
- The migration command never deletes profile files and never retires v2.
- Non-dev workspace writes require both `--workspace` and an exact
  `--i-understand-this-writes-to <workspace>` acknowledgement.

## Reversible Preparation

1. Make sure the current v2 stack and local Jarvis runtime are healthy enough to
   inspect, but do not write new cutover state yet.
2. Start a fresh Honcho v3 stack and point Jarvis at it:

   ```bash
   docker compose --env-file .env -f deploy/honcho-v3/docker-compose.v3.yml --profile gateway up -d
   ```

3. Configure the migration shell for v3:

   ```bash
   export MEMORY_BACKEND=v3
   export MEMORY_HOST=localhost
   export MEMORY_PORT=8003
   export MEMORY_MIGRATION_WORKSPACE_ID=jarvis-migration-dev
   ```

4. Dry-run the profile seed and inspect the exact conclusions:

   ```bash
   uv run jarvis memory-migrate \
     --users-dir jarvis-workspace/users \
     --workspace jarvis-migration-dryrun \
     --as-of YYYY-MM-DD \
     --dry-run
   ```

## Real Cutover Seed

Irreversible actions have not started yet. This stage writes only to a fresh v3
workspace.

1. Create or select the fresh v3 workspace for the real home memory:

   ```bash
   export MEMORY_WORKSPACE_ID=jarvis-home
   ```

2. Seed the declared rail into that fresh workspace:

   ```bash
   uv run jarvis memory-migrate \
     --users-dir jarvis-workspace/users \
     --workspace jarvis-home \
     --as-of YYYY-MM-DD \
     --i-understand-this-writes-to jarvis-home
   ```

   Each migrated fact is written with:

   - `level=explicit`
   - `recorded_by=<profile name>`
   - `source=profile-migration`
   - `observed_at=<as-of date>`
   - `content_hash=sha256:...`

3. Run the read-back gate again without writing:

   ```bash
   uv run jarvis memory-migrate \
     --users-dir jarvis-workspace/users \
     --workspace jarvis-home \
     --as-of YYYY-MM-DD \
     --verify \
     --i-understand-this-writes-to jarvis-home
   ```

4. The gate must report `Verification PASS` with the expected fact count before
   any profile files are retired or any v2 service is stopped.

## Irreversible Gate

Stop here unless a human explicitly confirms the cutover outcome and accepts the
rollback plan.

Irreversible steps:

- Point production Jarvis runtime env at the v3 `jarvis-home` workspace.
- Stop the old v2 runtime stack.
- Retire the profile files from the active declared rail.

Do not delete the v2 volume. Keep it as `jarvis-dev` history until a separate
human-reviewed cleanup says otherwise.

## Rollback Notes

Before the irreversible gate, rollback is just switching Jarvis env back to the
v2 memory service and workspace. After production env points at v3, rollback
requires restoring the previous env and restarting the affected Jarvis services.
The v2 volume must still exist for that rollback.

