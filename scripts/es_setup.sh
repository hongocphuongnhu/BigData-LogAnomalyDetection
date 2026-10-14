#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."
exec "${PYTHON:-python3}" -B scripts/es_setup.py
