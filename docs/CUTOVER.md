# Honcho v3 Cutover

Honcho v3 production cutover completed on 2026-07-05. Honcho v2 rollback
support was retired on 2026-07-19, and v3 is now Jarvis's only memory backend.

The old dual-stack runbook and rollback path are intentionally removed. The
canonical local stack is the root `docker-compose.yml`, which runs Honcho v3
alongside LiteLLM.

## Canonical Compose Volume Migration

The retired root compose used `honcho-pgdata` and `honcho-redis-data` for
Honcho v2. The new root compose uses explicit v3 volume names:

- `jarvis-honcho-v3-pgdata`
- `jarvis-honcho-v3-redis-data`

Do not start the new root compose against the old v2 volumes. On a host that was
already cut over to the temporary `deploy/honcho-v3` stack, copy the v3 data
into the canonical root-compose volumes before the first `docker compose up -d`
from the upgraded checkout.

1. Stop the temporary v3 stack and the old root-stack Honcho containers:

   ```bash
   docker rm -f jarvis-honcho-v3-api jarvis-honcho-v3-deriver jarvis-honcho-v3-db jarvis-honcho-v3-redis jarvis-litellm-v3 2>/dev/null || true
   docker rm -f jarvis-honcho-api jarvis-honcho-deriver jarvis-honcho-db jarvis-honcho-redis jarvis-litellm-v3 2>/dev/null || true
   ```

   If you are still on the old checkout before upgrading, you may stop the
   temporary stack with
   `docker compose --env-file .env -f deploy/honcho-v3/docker-compose.v3.yml --profile gateway down`
   instead.

   The old v2 root-stack containers used the same `container_name`s
   (`jarvis-honcho-api`, `jarvis-honcho-deriver`, `jarvis-honcho-db`,
   `jarvis-honcho-redis`). Removing them is expected; the new root compose
   recreates those names with the v3 image. `jarvis-litellm-v3` belonged only to
   the temporary validation/cutover stack and should be retired explicitly.

2. Find the existing temporary v3 data volumes. Compose project names may vary,
   so select the volumes by their v3 suffix:

   ```bash
   OLD_PG_VOLUME="$(docker volume ls --format '{{.Name}}' | grep 'honcho-v3-pgdata$' | head -n 1)"
   OLD_REDIS_VOLUME="$(docker volume ls --format '{{.Name}}' | grep 'honcho-v3-redis-data$' | head -n 1)"
   test -n "$OLD_PG_VOLUME"
   test -n "$OLD_REDIS_VOLUME"
   ```

3. Create the canonical v3 volumes:

   ```bash
   docker volume create jarvis-honcho-v3-pgdata
   docker volume create jarvis-honcho-v3-redis-data
   ```

4. Copy the temporary v3 data into the canonical volumes:

   ```bash
   docker run --rm \
     -v "$OLD_PG_VOLUME:/from:ro" \
     -v jarvis-honcho-v3-pgdata:/to \
     alpine sh -c 'cd /from && cp -a . /to/'

   docker run --rm \
     -v "$OLD_REDIS_VOLUME:/from:ro" \
     -v jarvis-honcho-v3-redis-data:/to \
     alpine sh -c 'cd /from && cp -a . /to/'
   ```

5. Remove the stale v2 volumes only after the v3 copy is complete and backed up:

   ```bash
   docker volume rm honcho-pgdata honcho-redis-data 2>/dev/null || true
   ```

6. Start the canonical stack:

   ```bash
   docker compose --env-file .env up -d
   ```

7. Verify the v3 data is present before using voice turns:

   ```bash
   curl -fsS "http://${MEMORY_HOST:-localhost}:${MEMORY_PORT:-8000}/health"
   uv run jarvis config
   uv run jarvis traces -n 5
   docker exec jarvis-honcho-db psql -U postgres -d postgres -c "select count(*) as honcho_documents from documents;"
   ```

   The health check must pass, `jarvis config` must point at the expected
   `memory.base_url`, recent traces should show successful cold-path memory
   refreshes, and the document count should match the temporary v3 stack before
   the cutover. A warm local representation cache should remain non-empty for
   known users; the hot path reads that cache and does not prove server data by
   itself.

Keep `MEMORY_CONCLUSION_SIDECAR_PATH` stable and backed up. Honcho v3.0.11
drops conclusion metadata on read-back, so Jarvis stores conclusion provenance
in the local sidecar at that path.
