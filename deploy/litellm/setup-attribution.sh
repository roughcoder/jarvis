#!/usr/bin/env bash
# Idempotently provision the LiteLLM observability/attribution hierarchy so the
# gateway logs separate every dimension. The keys created here are referenced by
# .env (GATEWAY_CLIENT_KEY, HONCHO_LLM_KEY), so this must be run once after the
# LiteLLM DB is created (and re-run if that DB is wiped).
#
# Dimension mapping (see README):
#   Team         = household           -> "jarvis"
#   Internal User= device / service    -> "jarvis-default", "honcho-memory"
#   Virtual Key  = component (alias)    -> "jarvis-voice", "honcho-memory"
#   End User     = resolved speaker/call owner (neil, alice, heartbeat; family fallback)
#   Tag          = room + Jarvis call dimensions (kind, channel, speaker, device)
#
# Usage: GATEWAY_API_KEY=sk-... ./deploy/litellm/setup-attribution.sh
set -euo pipefail

BASE="${GATEWAY_BASE_URL:-http://localhost:4000}"
MASTER="${GATEWAY_API_KEY:-sk-jarvis-local}"
auth=(-H "Authorization: Bearer ${MASTER}" -H "Content-Type: application/json")

echo "Provisioning LiteLLM attribution at ${BASE} …"

# 1) Household team — GET-OR-CREATE. /team/new is NOT idempotent (it makes a
# new team every call), so look it up first and only create if missing.
team_id_by_alias() {  # /team/list is a GET returning a bare list
  curl -s "${BASE}/team/list" "${auth[@]}" 2>/dev/null \
    | python3 -c "import sys,json;rows=json.load(sys.stdin);rows=rows if isinstance(rows,list) else rows.get('teams',[]);ids=[t['team_id'] for t in rows if t.get('team_alias')=='jarvis'];print(ids[0] if ids else '')"
}
TID=$(team_id_by_alias)
if [ -z "${TID}" ]; then
  curl -s -X POST "${BASE}/team/new" "${auth[@]}" -d '{"team_alias":"jarvis"}' >/dev/null 2>&1 || true
  TID=$(team_id_by_alias)
fi
if [ -z "${TID}" ]; then echo "  ERROR: could not resolve team 'jarvis'"; exit 1; fi
echo "  team jarvis -> ${TID}"

# 2) Internal users: per-device + shared memory service.
for uid in jarvis-default honcho-memory; do
  curl -s -X POST "${BASE}/user/new" "${auth[@]}" \
    -d "{\"user_id\":\"${uid}\",\"user_role\":\"internal_user\",\"auto_create_key\":false,\"teams\":[\"${TID}\"]}" >/dev/null 2>&1 || true
  echo "  user ${uid}"
done

# 3) Virtual keys (fixed values) owned by their user, in the household team.
provision_key() {  # value alias user_id
  curl -s -X POST "${BASE}/key/delete" "${auth[@]}" -d "{\"keys\":[\"$1\"]}" >/dev/null 2>&1 || true
  curl -s -X POST "${BASE}/key/generate" "${auth[@]}" \
    -d "{\"key\":\"$1\",\"key_alias\":\"$2\",\"team_id\":\"${TID}\",\"user_id\":\"$3\"}" >/dev/null
  echo "  key $2 (owner $3)"
}
provision_key sk-jarvis-voice   jarvis-voice   jarvis-default
provision_key sk-honcho-memory  honcho-memory  honcho-memory

echo "Done. Filter the Logs UI by Team / Key Alias / End User."
