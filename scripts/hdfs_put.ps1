# Dua du lieu local len HDFS; chay lai se cap nhat file, khong xoa data lake.
param(
    [ValidateSet("sim", "nasa", "all")][string]$Source = "sim"
)
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

function Invoke-Hdfs {
    param([string[]]$HdfsArgs)
    & docker compose exec -T namenode hdfs @HdfsArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Lenh HDFS that bai (exit $LASTEXITCODE): hdfs $($HdfsArgs -join ' ')"
    }
}

$uploadSim = $Source -in @("sim", "all")
$uploadNasa = $Source -in @("nasa", "all")
if ($uploadSim -and -not (Get-ChildItem -Path "data/bronze/http_access" -Filter "*.jsonl" -Recurse -ErrorAction SilentlyContinue)) {
    throw "Chua co bronze simulator. Chay python -m generator.simulate truoc."
}
if ($uploadNasa -and -not (Test-Path -LiteralPath "data/raw/nasa/NASA_access_log_Jul95.gz" -PathType Leaf)) {
    throw "Chua co NASA_access_log_Jul95.gz. Doi du lieu Phase 02 hoac dung -Source sim."
}

Invoke-Hdfs -HdfsArgs @("dfs", "-mkdir", "-p", "/lake/bronze", "/lake/raw/nasa", "/lake/silver", "/lake/gold")
if ($uploadSim) {
    Invoke-Hdfs -HdfsArgs @("dfs", "-put", "-f", "/data/bronze/http_access", "/lake/bronze/")
}
if ($uploadNasa) {
    Invoke-Hdfs -HdfsArgs @("dfs", "-put", "-f", "/data/raw/nasa/NASA_access_log_Jul95.gz", "/lake/raw/nasa/")
}
Invoke-Hdfs -HdfsArgs @("dfs", "-ls", "/lake")
Write-Host "HDFS upload OK ($Source)."
