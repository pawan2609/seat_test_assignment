#!/usr/bin/env bash
# Usage: ./burst.sh <BASE_URL> [ADMIN_TOKEN] [extra burst flags...]
# Runs the stampede against any deployment. Uses a local Go toolchain if
# present, otherwise builds and runs the burst tool in Docker.
set -euo pipefail
if [[ $# -lt 1 ]]; then
  echo "usage: $0 <BASE_URL> [ADMIN_TOKEN] [flags]   (or set ADMIN_TOKEN)" >&2
  exit 2
fi
BASE_URL=$1; shift
ADMIN=${ADMIN_TOKEN:-}
if [[ $# -gt 0 && $1 != -* ]]; then ADMIN=$1; shift; fi
if [[ -z $ADMIN ]]; then echo "admin token required (arg 2 or ADMIN_TOKEN)" >&2; exit 2; fi

cd "$(dirname "$0")"
ulimit -n 65536 2>/dev/null || true
if command -v go >/dev/null 2>&1; then
  exec go run ./cmd/burst -admin "$ADMIN" "$@" "$BASE_URL"
fi
docker build -q -t seat-reservation-burst . >/dev/null
exec docker run --rm --network host --ulimit nofile=65536:65536 \
  --entrypoint burst seat-reservation-burst -admin "$ADMIN" "$@" "$BASE_URL"
