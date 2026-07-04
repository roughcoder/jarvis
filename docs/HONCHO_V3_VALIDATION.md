# Honcho v3 LiteLLM Validation

Date: 2026-07-04

## Verdict

Honcho `v3.0.11` can route the tested LLM surfaces through LiteLLM's
OpenAI-compatible gateway when each v3 model config uses `transport=openai`, a
LiteLLM route name as `model`, and `overrides.base_url` pointed at LiteLLM.

The v2 `custom` provider name is gone in v3. The v3 equivalent is the OpenAI
transport plus per-caller endpoint overrides.

Validated surfaces:

- Deriver: passed, with `structured_output_mode=json_object`.
- Dialectic chat: passed.
- Summaries: passed.
- Dreamer deduction and induction specialists: passed.
- Message/search embeddings: passed.
- Explicit conclusion create/list/delete: passed, with caveats below.
- `/health` and `/v3/workspaces/{id}/queue/status`: passed.

## Dev Stack

Dev-only compose lives at:

```bash
deploy/honcho-v3/docker-compose.v3.yml
```

Default command, for an already-running host LiteLLM gateway on port 4000:

```bash
docker compose --env-file .env -f deploy/honcho-v3/docker-compose.v3.yml up -d
```

Isolated validation command, using the repo LiteLLM config with separate
containers, DB, and volumes:

```bash
HONCHO_V3_LITELLM_BASE_URL=http://litellm-v3:4000/v1 \
  docker compose --env-file .env -f deploy/honcho-v3/docker-compose.v3.yml --profile gateway up -d
```

Run validation:

```bash
python3 deploy/honcho-v3/validate.py
```

Stop the dev stack:

```bash
docker compose --env-file .env -f deploy/honcho-v3/docker-compose.v3.yml --profile gateway down
```

## Required Config

Use LiteLLM route names, not provider-native model ids:

- `HONCHO_V3_LITELLM_CHAT_MODEL=honcho-llm`
- `HONCHO_V3_LITELLM_EMBED_MODEL=embed`
- `HONCHO_V3_LITELLM_BASE_URL=http://host.docker.internal:4000/v1` for an
  existing host gateway, or `http://litellm-v3:4000/v1` for the isolated
  compose profile.
- `HONCHO_V3_LITELLM_KEY` optional. If unset, compose falls back to the
  existing `HONCHO_LLM_KEY`, then `sk-honcho-memory`.

Honcho v3 settings used by the compose:

- `LLM_OPENAI_API_KEY`: a LiteLLM key accepted by the gateway.
- `DERIVER_MODEL_CONFIG__MODEL=honcho-llm`
- `DERIVER_MODEL_CONFIG__OVERRIDES__BASE_URL=<LiteLLM /v1 URL>`
- `DERIVER_MODEL_CONFIG__STRUCTURED_OUTPUT_MODE=json_object`
- `DIALECTIC_LEVELS__{minimal,low,medium,high,max}__MODEL_CONFIG__MODEL=honcho-llm`
- `DIALECTIC_LEVELS__{minimal,low,medium,high,max}__MODEL_CONFIG__OVERRIDES__BASE_URL=<LiteLLM /v1 URL>`
- `SUMMARY_MODEL_CONFIG__MODEL=honcho-llm`
- `SUMMARY_MODEL_CONFIG__OVERRIDES__BASE_URL=<LiteLLM /v1 URL>`
- `DREAM_DEDUCTION_MODEL_CONFIG__MODEL=honcho-llm`
- `DREAM_DEDUCTION_MODEL_CONFIG__OVERRIDES__BASE_URL=<LiteLLM /v1 URL>`
- `DREAM_INDUCTION_MODEL_CONFIG__MODEL=honcho-llm`
- `DREAM_INDUCTION_MODEL_CONFIG__OVERRIDES__BASE_URL=<LiteLLM /v1 URL>`
- `EMBEDDING_MODEL_CONFIG__MODEL=embed`
- `EMBEDDING_MODEL_CONFIG__OVERRIDES__BASE_URL=<LiteLLM /v1 URL>`
- `EMBEDDING_VECTOR_DIMENSIONS=1536`
- `EMBED_MESSAGES=true`

Runtime config confirmed inside the API container:

```text
deriver honcho-llm http://litellm-v3:4000/v1 json_object
embedding embed http://litellm-v3:4000/v1
summary honcho-llm http://litellm-v3:4000/v1
dream_deduction honcho-llm http://litellm-v3:4000/v1
dream_induction honcho-llm http://litellm-v3:4000/v1
dialectic minimal/low/medium/high/max honcho-llm http://litellm-v3:4000/v1
```

## Evidence

Validation command:

```bash
python3 deploy/honcho-v3/validate.py
```

Key output from the passing run:

```text
health=ok
messages_created=12
new_conclusions=6
queue_idle[messages]=... pending_work_units: 0
conclusions_after_deriver=21
short_summary_present=True
dialectic_chat='The validation keyword is **copper-lantern**.'
explicit_conclusion_crud=ok
queue_idle[dream]=... pending_work_units: 0
litellm_log_counts={"chat_completions": 16, "embeddings": 16}
validation=ok
```

Honcho deriver logs showed:

- `minimal_deriver... observation_count=11`
- `summary_jarvis-dev... SHORT_summary_creation=1673ms`
- `dreamer_deduction... tool_calls=5`
- `dreamer_induction... tool_calls=4`
- `dream_orchestrator... Dream cycle completed`

LiteLLM logs showed repeated:

- `POST /v1/chat/completions HTTP/1.1" 200 OK`
- `POST /v1/embeddings HTTP/1.1" 200 OK`

LiteLLM DB evidence from `LiteLLM_SpendLogs`:

```text
model                         count
openai/gpt-4o-mini            16
openai/text-embedding-3-small  16
```

Honcho DB evidence after validation:

```text
messages: 36
documents/conclusions: 33
message_embeddings synced: 36
conclusion levels: explicit=25, deductive=5, inductive=3
```

## Gotchas

`structured_output_mode=json_object` is required for the deriver with this
LiteLLM route. Honcho v3 defaults to OpenAI Structured Outputs
(`json_schema`) for structured model calls; the v3 source explicitly warns
that OpenAI-compatible providers may reject that request shape and should use
`json_object`.

Resource IDs cannot contain colons in v3. `WorkspaceCreate`, `PeerCreate`, and
`SessionCreate` validate ids against `^[a-zA-Z0-9_-]+$`. The first validation
attempt with `validation:<timestamp>` failed with HTTP 422. Jarvis's planned
ids like `voice:<person>:<device>` and `project:<id>` need an encoding layer
before the v3 client lands.

Explicit conclusions require both peers to exist first. Creating a conclusion
for `project-jarvis` failed with HTTP 404 until the project peer was created.

Conclusion metadata does not round-trip in v3.0.11. The source schema for
`ConclusionCreate` has only `content`, `observer_id`, `observed_id`, and
optional `session_id`; an actual request containing extra `metadata` returned
201 but silently dropped the metadata in the response. Lane 2 still needs a
mitigation before implementation: either upstream metadata support for
conclusions, or a Jarvis-owned sidecar keyed by Honcho conclusion id. Do not
encode provenance in prose as the durable answer.

`/queue/status` is useful after enqueue is visible, but it can report zero
immediately after `POST /messages` because message enqueue runs as a FastAPI
background task. The validation runner waits for the conclusion count to
increase, then checks queue idle.

Dream timing settings must be positive. `DREAM_IDLE_TIMEOUT_MINUTES=0` and
`DREAM_MIN_HOURS_BETWEEN_DREAMS=0` fail pydantic validation at startup; the dev
compose uses `1`.

## Recommendation

Proceed with Build order step 2 only if the v3 client includes the encoding
layer for Honcho resource names and explicitly handles conclusion provenance.
LiteLLM routing itself is validated for v3.0.11.
