#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")"

mkdir -p ./data ./logs ./exports ./import

STOP_FILE="$(pwd)/data/yt-assist.stop"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  if command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
  else
    echo "No Python interpreter was found for PebbleHost startup."
    exit 1
  fi
fi

request_graceful_shutdown() {
  signal_name="$1"
  timestamp="$(date -Iseconds 2>/dev/null || date)"
  printf '%s %s\n' "$timestamp" "$signal_name" > "$STOP_FILE"
  echo "PebbleHost requested shutdown via $signal_name. Waiting for yt-assist to stop cleanly..."
}

trap 'request_graceful_shutdown SIGTERM' TERM
trap 'request_graceful_shutdown SIGINT' INT
trap 'request_graceful_shutdown SIGHUP' HUP

rm -f "$STOP_FILE"

echo "Starting yt-assist via PebbleHost wrapper..."
"$PYTHON_BIN" -u bot.py &
BOT_PID=$!

while :; do
  if wait "$BOT_PID"; then
    exit 0
  fi
  exit_code=$?
  if ! kill -0 "$BOT_PID" 2>/dev/null; then
    exit "$exit_code"
  fi
done
