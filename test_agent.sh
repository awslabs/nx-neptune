#!/usr/bin/env bash
# Manual test script for the nx-neptune projection agent (/api/v0/agent).
#
# Prereqs:
#   - Proxy running locally (see step 1 below). It prints a launch URL with a
#     token: http://127.0.0.1:8080/?token=XXXX
#   - AWS creds on this machine with Bedrock (model enabled in your region) and
#     Athena access.
#
# Usage:
#   1) Start the proxy in another terminal:
#        cd proxy && AWS_REGION=us-west-2 make run      # or your run target
#   2) Export the token it printed:
#        export TOKEN=<the token from the launch URL>
#   3) Run this script:
#        ./test_agent.sh
#
# The conversation is STATEFUL (single shared session), so turns build on each
# other. Use the reset call at the end to start over.

set -euo pipefail

BASE="${BASE:-http://127.0.0.1:8080}"
# Accept either `token` (lowercase) or `TOKEN`; lowercase wins if both are set.
TOKEN="${token:-${TOKEN:-}}"
: "${TOKEN:?Set token (or TOKEN) to the token from the proxy launch URL, e.g. export token=...}"

# Every state-changing request needs all three: auth token, CSRF header, JSON.
AUTH=(-H "Authorization: Bearer ${TOKEN}")
CSRF=(-H "X-Requested-With: nx-neptune")
JSON=(-H "Content-Type: application/json")

chat() {
  # $1 = message text
  echo ">>> USER: $1"
  curl -sS "${BASE}/api/v0/agent/chat" \
    "${AUTH[@]}" "${CSRF[@]}" "${JSON[@]}" \
    -d "$(printf '{"message": %s}' "$(printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')")" \
    | (jq -r '.reply // .' 2>/dev/null || cat)
  echo
  echo "-----------------------------------------------------------------"
}

echo "=== 0. Guardrail checks (no AWS needed) ==="
echo -n "no CSRF header  -> expect 403: "
curl -sS -o /dev/null -w "%{http_code}\n" "${BASE}/api/v0/agent/chat" \
  "${AUTH[@]}" "${JSON[@]}" -d '{"message":"hi"}'
echo -n "no auth token   -> expect 401/403: "
curl -sS -o /dev/null -w "%{http_code}\n" "${BASE}/api/v0/agent/chat" \
  "${CSRF[@]}" "${JSON[@]}" -d '{"message":"hi"}'
echo -n "empty message   -> expect 422: "
curl -sS -o /dev/null -w "%{http_code}\n" "${BASE}/api/v0/agent/chat" \
  "${AUTH[@]}" "${CSRF[@]}" "${JSON[@]}" -d '{"message":""}'
echo "-----------------------------------------------------------------"

echo "=== 1. Discover: which databases exist? (tier 1: list_databases) ==="
chat "I want to build a graph from my data lake. What databases are available?"

echo "=== 2. Inspect + compose: pick a DB and propose SQL (tier 2: get_schema) ==="
echo "    (edit the database name below to one that exists in your account)"
chat "Use the fraud database. Model accounts as nodes and the transactions between them as edges, and propose the node and edge SQL."

echo "=== 3. Refine (optional) — the session remembers the above ==="
chat "Add the transaction amount as an edge property."

echo "=== 4. Approve -> create DRAFT (agent calls create_projection_draft) ==="
chat "Looks good. Create the draft projection."

echo "=== 5. Confirm the draft landed (status should be 'draft') ==="
curl -sS "${BASE}/api/v0/projection" "${AUTH[@]}" \
  | (jq '[.[] | {id, status, database, graph_name}]' 2>/dev/null || cat)
echo "-----------------------------------------------------------------"

echo "=== 6. Reset the conversation (start fresh) ==="
curl -sS -X POST "${BASE}/api/v0/agent/reset" \
  "${AUTH[@]}" "${CSRF[@]}" | (jq '.' 2>/dev/null || cat)
