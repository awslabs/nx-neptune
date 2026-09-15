#!/usr/bin/env bash
# Interactive REPL for the nx-neptune projection agent (/api/v0/agent).
#
# Holds an ongoing conversation with the STATEFUL single-session agent so you
# can test multi-turn discover -> drill -> propose -> create flows by hand.
#
# Commands (type at the "you>" prompt):
#   flush   -> reset the conversation (POST /api/v0/agent/reset), start fresh
#   list    -> show current projections (GET /api/v0/projection)
#   quit    -> exit (also: exit, Ctrl-D)
#   <text>  -> send as a chat message to the agent
#
# Prereqs:
#   - Proxy running locally (prints a launch URL with ?token=XXXX)
#   - AWS creds with Bedrock + Athena
#
# Usage:
#   export TOKEN=<token from the proxy launch URL>
#   ./chat_repl.sh

set -uo pipefail

BASE="${BASE:-http://127.0.0.1:8080}"
# Accept either `token` (lowercase) or `TOKEN`; lowercase wins if both are set.
TOKEN="${token:-${TOKEN:-}}"
: "${TOKEN:?Set token (or TOKEN) to the token from the proxy launch URL, e.g. export token=...}"

AUTH=(-H "Authorization: Bearer ${TOKEN}")
CSRF=(-H "X-Requested-With: nx-neptune")
JSON=(-H "Content-Type: application/json")

# JSON-encode an arbitrary string safely (handles quotes, newlines, etc.).
json_encode() {
  printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'
}

send_chat() {
  local msg="$1"
  local body
  body="$(printf '{"message": %s}' "$(json_encode "$msg")")"
  local resp
  resp="$(curl -sS "${BASE}/api/v0/agent/chat" "${AUTH[@]}" "${CSRF[@]}" "${JSON[@]}" -d "$body")"
  # Pretty-print the reply, or dump raw JSON on error.
  printf '%s' "$resp" | jq -r '.reply // .detail // .' 2>/dev/null || printf '%s\n' "$resp"
}

flush_conversation() {
  curl -sS -X POST "${BASE}/api/v0/agent/reset" "${AUTH[@]}" "${CSRF[@]}" \
    | (jq -r '.status // .' 2>/dev/null || cat)
}

list_projections() {
  curl -sS "${BASE}/api/v0/projection" "${AUTH[@]}" \
    | (jq '[.[] | {id, status, database, graph_name}]' 2>/dev/null || cat)
}

echo "nx-neptune agent REPL — talking to ${BASE}"
echo "commands: flush (reset)  |  list (show projections)  |  quit"
echo "-----------------------------------------------------------------"

while true; do
  # -r: don't mangle backslashes; read a full line into $line
  if ! IFS= read -r -p $'\nyou> ' line; then
    echo; echo "bye."; break            # Ctrl-D / EOF
  fi

  case "$line" in
    "" )
      continue ;;                        # empty line: ignore
    flush )
      echo -n "… resetting conversation: "
      flush_conversation ;;
    list )
      list_projections ;;
    quit|exit )
      echo "bye."; break ;;
    * )
      echo
      echo "agent>"
      send_chat "$line" ;;
  esac
done
