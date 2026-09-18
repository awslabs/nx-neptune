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
# Rendering:
#   Replies are rendered as markdown if `glow` (or `bat`/`batcat`) is installed,
#   so **bold** and lists look right in the terminal. Falls back to plain text
#   otherwise. Set NO_MD=1 to force plain text.
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

# Render a reply string to the terminal. If a markdown renderer is available
# (glow, or bat/batcat), use it so **bold**, lists, and arrows look nicer;
# otherwise print the raw text unchanged. Set NO_MD=1 to force plain text.
# Render markdown-ish text nicely in the terminal. Prefers glow/bat if present;
# otherwise falls back to a dependency-free formatter (awk) that:
#   - renders **bold**/__bold__ with ANSI bold (or strips it if not a TTY)
#   - renders `code` with ANSI dim
#   - normalizes "- "/"* " bullets to "  • " and preserves indentation
#   - lightens "# " headings to bold text
#   - preserves blank lines / line breaks
# Set NO_MD=1 to force raw text.
render_reply() {
  local text="$1"
  if [[ "${NO_MD:-}" == "1" ]]; then
    printf '%s\n' "$text"; return
  fi
  if command -v glow >/dev/null 2>&1; then
    printf '%s' "$text" | glow - 2>/dev/null && return
  elif command -v bat >/dev/null 2>&1; then
    printf '%s' "$text" | bat --language=md --style=plain --paging=never 2>/dev/null && return
  elif command -v batcat >/dev/null 2>&1; then
    printf '%s' "$text" | batcat --language=md --style=plain --paging=never 2>/dev/null && return
  fi

  # Fallback: use ANSI only when writing to a terminal.
  local b="" d="" r=""
  if [[ -t 1 ]]; then
    b=$'\033[1m'; d=$'\033[2m'; r=$'\033[0m'
  fi

  printf '%s\n' "$text" | awk -v B="$b" -v D="$d" -v R="$r" '
    {
      line = $0

      # Headings: "### Foo" / "# Foo" -> bold, no hashes
      if (match(line, /^[[:space:]]*#+[[:space:]]+/)) {
        sub(/^[[:space:]]*#+[[:space:]]+/, "", line)
        line = B line R
        print line
        next
      }

      # Bullets: leading "- " or "* " -> "  • " (preserve as UTF-8 bullet)
      if (match(line, /^[[:space:]]*[-*][[:space:]]+/)) {
        rest = substr(line, RLENGTH + 1)
        line = "  \342\200\242 " rest
      }

      # Bold: **text** and __text__
      while (match(line, /\*\*[^*]+\*\*/)) {
        seg = substr(line, RSTART+2, RLENGTH-4)
        line = substr(line, 1, RSTART-1) B seg R substr(line, RSTART+RLENGTH)
      }
      while (match(line, /__[^_]+__/)) {
        seg = substr(line, RSTART+2, RLENGTH-4)
        line = substr(line, 1, RSTART-1) B seg R substr(line, RSTART+RLENGTH)
      }

      # Inline code: `text` -> dim
      while (match(line, /`[^`]+`/)) {
        seg = substr(line, RSTART+1, RLENGTH-2)
        line = substr(line, 1, RSTART-1) D seg R substr(line, RSTART+RLENGTH)
      }

      print line
    }
  '
}

send_chat() {
  local msg="$1"
  local body
  body="$(printf '{"message": %s}' "$(json_encode "$msg")")"
  local resp
  resp="$(curl -sS "${BASE}/api/v0/agent/chat" "${AUTH[@]}" "${CSRF[@]}" "${JSON[@]}" -d "$body")"
  # Extract the reply (or error), then render it (markdown if a renderer exists).
  local reply
  if reply="$(printf '%s' "$resp" | jq -r '.reply // .detail // .' 2>/dev/null)"; then
    render_reply "$reply"
  else
    printf '%s\n' "$resp"
  fi
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
