$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
python -B scripts/es_setup.py
exit $LASTEXITCODE
