#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")"

PYTHON_BIN="${PYTHON_BIN:-python3}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  if command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
  else
    echo "No Python interpreter was found for PebbleHost startup."
    exit 1
  fi
fi

echo "Starting managed YouTool + Bakunawa Mech launcher..."
exec "$PYTHON_BIN" -u bot.py
