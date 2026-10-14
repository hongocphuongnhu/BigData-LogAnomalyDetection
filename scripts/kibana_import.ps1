param([string]$File = "kibana/dashboards.ndjson")
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
python -B scripts/kibana_import.py --file $File
exit $LASTEXITCODE
