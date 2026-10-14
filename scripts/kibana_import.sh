#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."
exec "${PYTHON:-python3}" -B scripts/kibana_import.py --file "${1:-kibana/dashboards.ndjson}"
