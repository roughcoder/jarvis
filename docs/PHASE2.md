# Jarvis — Phase 2 Spec: Frankfurt Migration

> Phase 2 is **purely a relocation**, kept boring on purpose. Move the heavy,
> always-on memory tier off Hive (the Mac at home) and onto the Frankfurt
> Hetzner box, leaving the entire voice hot path at home untouched. If Phase 1
> was built as verified — env-driven, hot/cold split intact — this is a config
> change, not a rebuild. Do not add features here.

## 1. Objective

Same Jarvis, same felt latency, but **memory + its database now live in
Frankfurt**; Hive runs only the voice loop + gateway; the two are linked over
Tailscale; the user notices nothing.

## 2. What moves vs what stays

**Moves to Frankfurt:** Honcho (api + deriver) **and its Postgres/pgvector**.
These are the always-on, latency-insensitive pieces — writes are async, the
deriver reasons between turns, nobody waits on them. Honcho and its DB move
**together as a unit** (Honcho reaches the DB over its compose-internal URI).

**Stays at home (Hive), permanently:** the whole hot path — wake word, VAD, STT,
the turn loop, TTS playback — **and the LiteLLM gateway**. The gateway staying at
the edge is deliberate: LLM calls go home→provider directly, never bouncing
through Frankfurt, so no 750 km detour per turn. Honcho's own reasoning calls go
Frankfurt→home-gateway→provider (acceptable — they're cold-path/background).

## 3. The one load-bearing change

On Hive, exactly **one** variable changes: **`MEMORY_HOST`** from `localhost` to
the Frankfurt Tailscale hostname (and `MEMORY_PORT` if not 8000).

`DB_HOST` is **not** load-bearing on Hive — Jarvis never connects to Postgres
directly; only Honcho does, and the DB moves with Honcho. Don't go looking for a
`DB_HOST` wire in the app; it isn't plumbed.

## 4. Entry gate — the readiness test (do this BEFORE touching Frankfurt)

Prove the hot/cold split holds locally, or the move will silently make Jarvis
slow. Point memory at a dead boundary and confirm:

```bash
MEMORY_PORT=1 uv run python - <<'PY'
from jarvis.config import load_config
from jarvis.memory_client import MemoryClient
mc = MemoryClient(load_config().memory)
print("hot read:", len(mc.read_cached_representation()))   # WORKS (local, no net)
for fn in (lambda: mc._write_turn_sync("a","b"), mc._refresh_cache_sync):
    try: fn(); print("COLD SUCCEEDED — co-location shortcut?!")
    except Exception as e: print("cold failed at boundary:", type(e).__name__)
PY
```

**Gate:** hot-path cache read works; both cold calls fail cleanly at the
boundary (ConnectError, fast — not a hang, not a silent half-success). If
anything on the hot path needs the network, find and remove the co-location
shortcut before migrating. (Verified passing at end of Phase 1.)

## 5. Migration steps

1. **Provision Frankfurt.** Bring up Honcho (api + deriver) + Postgres/pgvector
   + redis on the Hetzner box using the *same* `docker-compose.yml` services
   (just the `honcho-*` stack; litellm stays on Hive). Same env, except the
   deriver's `LLM_OPENAI_COMPATIBLE_BASE_URL` now points at Hive's gateway over
   Tailscale (`http://<hive-tailscale>:4000/v1`) instead of `litellm:4000`.
2. **Data: migrate or fresh.** Decide explicitly:
   - *Fresh start* — simplest; Jarvis re-learns from new conversations. Honcho's
     long-term memory rebuilds over time. Fine for a single user.
   - *Migrate* — `pg_dump` the Hive `honcho` DB and restore into Frankfurt's, to
     carry existing memory across. Do this if the accumulated representation
     matters.
3. **Repoint Hive.** Change `MEMORY_HOST` (and `MEMORY_PORT` if needed) in Hive's
   `.env` to the Frankfurt Tailscale hostname. Restart nothing but the app.
4. **Tailscale.** Both ends are already on the tailnet; no new auth or public
   exposure — Honcho's API is reached over Tailscale only.
5. Re-run `deploy/litellm/setup-attribution.sh` against Frankfurt's Honcho only
   if its LiteLLM-side keys changed (they don't — the gateway is unchanged).

## 6. Definition of done

- `jarvis run` behaves identically; felt latency unchanged (the readiness test
  guarantees the hot path never waits on Frankfurt).
- Memory + DB run in Frankfurt; Hive runs only the voice loop + gateway.
- Confirm via `jarvis traces`: per-turn `stt/llm/tts` timings are unchanged, and
  the `(cold path)` memory line still lands after the reply — now just taking a
  Tailscale round-trip, invisibly.

## 7. What Phase 2 is NOT

Not multi-user household peers, document RAG, or non-voice surfaces — those are
Phase 3, and they're what justify Frankfurt being a proper always-on tier rather
than "Postgres, relocated." Keep Phase 2 to the relocation.

## 8. Watch-item carried from Phase 1

Cold-path/VAD contention in the follow-up window (see AGENTS.md). Moving Honcho
to Frankfurt **reduces local Docker CPU contention on Hive** (the deriver no
longer runs on the Mac), so if Phase 1 showed follow-up twitchiness from
contention, Phase 2 may quietly improve it. Confirm with the trace timeline.
