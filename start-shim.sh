#!/usr/bin/env bash
# Starts the neura gateway if it isn't already listening.
# Safe to call from .bashrc or a cron line — probes first, never double-starts.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${NW_SHIM_PORT:-8787}"
LOG="${NEURA_LOG:-$DIR/neura.log}"

curl -s -o /dev/null -m 2 "http://127.0.0.1:${PORT}/v1/models" && exit 0

setsid nohup python3 "$DIR/neura.py" >> "$LOG" 2>&1 < /dev/null &
for _ in $(seq 1 15); do
  curl -s -o /dev/null -m 2 "http://127.0.0.1:${PORT}/v1/models" && exit 0
  sleep 1
done
echo "neura failed to start — see $LOG" >&2
exit 1