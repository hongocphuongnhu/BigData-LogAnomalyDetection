# Tai NASA HTTP access log thang 7/1995 (~20MB nen, 1.891.714 dong) vao data/raw/nasa/.
# Nguon: https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
New-Item -ItemType Directory -Force data/raw/nasa | Out-Null
$out = "data/raw/nasa/NASA_access_log_Jul95.gz"
if ((Test-Path $out) -and ((Get-Item $out).Length -gt 0)) { Write-Host "Da co $out, bo qua."; exit 0 }
Invoke-WebRequest -Uri "https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz" -OutFile $out
Write-Host "Da tai $out"
