#!/usr/bin/env sh
set -eu
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."
exec "${PYTHON:-python3}" -B scripts/run_all.py "$@"
