#!/usr/bin/env bash
#
# Local Clipboard shell wrapper
#
# Source this file to load helper functions for the Local Clipboard HTTP API:
#
#   source scripts/local-clipboard.sh
#
# Then configure your target server/room with:
#
#   lc_set_env [HOST] [ROOM] [SCHEME]
#
# Empty arguments keep the defaults:
#   HOST   -> localhost:8080
#   ROOM   -> default room
#   SCHEME -> http
#

# Default environment values
: "${LOCAL_CLIPBOARD_HOST:=localhost:8080}"
: "${LOCAL_CLIPBOARD_ROOM:=}"
: "${LOCAL_CLIPBOARD_SCHEME:=http}"

_lc_base_url() {
    local room_path=""
    if [ -n "$LOCAL_CLIPBOARD_ROOM" ]; then
        room_path="/r/$LOCAL_CLIPBOARD_ROOM"
    fi
    echo "${LOCAL_CLIPBOARD_SCHEME}://${LOCAL_CLIPBOARD_HOST}${room_path}"
}

lc_set_env() {
    LOCAL_CLIPBOARD_HOST="${1:-localhost:8080}"
    LOCAL_CLIPBOARD_ROOM="${2:-}"
    LOCAL_CLIPBOARD_SCHEME="${3:-http}"
    export LOCAL_CLIPBOARD_HOST LOCAL_CLIPBOARD_ROOM LOCAL_CLIPBOARD_SCHEME
    echo "local-clipboard env: ${LOCAL_CLIPBOARD_SCHEME}://${LOCAL_CLIPBOARD_HOST} room=${LOCAL_CLIPBOARD_ROOM:-default}"
}

lc_help() {
    cat <<'EOF'
Local Clipboard shell wrapper

Environment variables (with defaults):
  LOCAL_CLIPBOARD_HOST    localhost:8080
  LOCAL_CLIPBOARD_ROOM    (empty -> default room)
  LOCAL_CLIPBOARD_SCHEME  http

Functions:
  lc_set_env [HOST] [ROOM] [SCHEME]   Set server/room defaults
  lc_send_text "message"              Send a text message
  lc_send_file /path/to/file          Upload a file
  lc_messages                         List room messages and file URLs
  lc_download_all                     Download every file in the room
  lc_clear                            Clear all messages and files
  lc_set_interval N                   Set auto-clear interval in minutes
  lc_pause                            Pause the auto-clear timer
  lc_resume                           Resume the auto-clear timer
  lc_version                          Show server version
  lc_help                             Show this help
EOF
}

lc_send_text() {
    local text="$*"
    local url="$(_lc_base_url)/api/send"

    if command -v jq >/dev/null 2>&1; then
        curl -s -X POST "$url" \
            -H "Content-Type: application/json" \
            -d "$(jq -n --arg text "$text" '{text: $text}')"
    else
        curl -s -X POST "$url" \
            -H "Content-Type: application/json" \
            -d "{\"text\":\"$text\"}"
    fi
    echo
}

lc_send_file() {
    local file="$1"
    if [ -z "$file" ]; then
        echo "Usage: lc_send_file /path/to/file" >&2
        return 1
    fi
    if [ ! -f "$file" ]; then
        echo "File not found: $file" >&2
        return 1
    fi

    local url="$(_lc_base_url)/api/send"
    curl -s -X POST "$url" -F "file=@$file"
    echo
}

lc_messages() {
    local url="$(_lc_base_url)/api/messages"
    if command -v jq >/dev/null 2>&1; then
        curl -s "$url" | jq .
    else
        curl -s "$url"
    fi
}

lc_download_all() {
    local url="$(_lc_base_url)/api/messages"
    if command -v jq >/dev/null 2>&1; then
        curl -s "$url" | jq -r '.files[].url' | xargs -n1 curl -LO
    else
        echo "lc_download_all requires jq to parse file URLs" >&2
        return 1
    fi
}

lc_clear() {
    local url="$(_lc_base_url)/clear"
    curl -s -X POST "$url"
    echo "Cleared room: ${LOCAL_CLIPBOARD_ROOM:-default}"
}

lc_set_interval() {
    local minutes="$1"
    if ! [[ "$minutes" =~ ^[0-9]+$ ]]; then
        echo "Usage: lc_set_interval N  (N must be a non-negative integer)" >&2
        return 1
    fi

    local url="$(_lc_base_url)/set-interval"
    curl -s -X POST "$url" \
        -H "Content-Type: application/json" \
        -d "{\"interval\":$minutes}"
    echo "Auto-clear interval set to $minutes minute(s)"
}

lc_pause() {
    local url="$(_lc_base_url)/toggle-pause"
    curl -s -X POST "$url"
    echo "Toggled pause for room: ${LOCAL_CLIPBOARD_ROOM:-default}"
}

lc_resume() {
    # The server toggles pause state; resume and pause call the same endpoint.
    lc_pause
}

lc_version() {
    local url="$(_lc_base_url)/api/version"
    curl -s "$url"
    echo
}

# If executed directly instead of sourced, print usage.
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    lc_help
fi
