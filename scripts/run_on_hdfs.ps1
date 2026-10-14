# Dung job batch chung, chi doi duong dan sang HDFS.
param(
    [ValidateSet("sim", "nasa")][string]$Source = "sim",
    [switch]$BuildFeatures
)
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
$lakeRoot = "hdfs://namenode:9000/lake"

function Invoke-SparkOnHdfs {
    param([string]$Job, [string[]]$JobArgs)
    # dfs.replication la cau hinh CLIENT: NameNode dat 1 khong doi mac dinh 3 cua Spark.
    & docker compose exec -T spark-master /opt/spark/bin/spark-submit `
        --master spark://spark-master:7077 `
        --conf spark.sql.shuffle.partitions=16 `
        --conf spark.hadoop.dfs.replication=1 `
        --py-files /opt/spark/jobs/features_common.py "/opt/spark/jobs/$Job" @JobArgs
    if ($LASTEXITCODE -ne 0) { throw "Spark job $Job that bai (exit $LASTEXITCODE)." }
}

if ($Source -eq "sim") {
    $job = "ingest_http_access.py"
    $jobArgs = @(
        "--bronze-path", "$lakeRoot/bronze/http_access",
        "--silver-path", "$lakeRoot/silver/http_access",
        "--rejected-path", "$lakeRoot/silver/_rejected/http_access"
    )
} else {
    $job = "ingest_nasa.py"
    $jobArgs = @(
        "--raw-path", "$lakeRoot/raw/nasa/NASA_access_log_Jul95.gz",
        "--silver-path", "$lakeRoot/silver/nasa_access",
        "--rejected-path", "$lakeRoot/silver/_rejected/nasa_access"
    )
}
# Kiem tra job chua duoc ban giao truoc khi chay bat ky buoc nao.
if (-not (Test-Path -LiteralPath "jobs/$job" -PathType Leaf)) {
    throw "Chua co jobs/$job. Doi Dinh ban giao job Phase 02."
}
if ($BuildFeatures -and -not (Test-Path -LiteralPath "jobs/build_features.py" -PathType Leaf)) {
    throw "Chua co jobs/build_features.py. Doi Dinh ban giao job Phase 03."
}

Invoke-SparkOnHdfs -Job $job -JobArgs $jobArgs
if ($BuildFeatures) {
    $silverName = if ($Source -eq "sim") { "http_access" } else { "nasa_access" }
    Invoke-SparkOnHdfs -Job "build_features.py" -JobArgs @(
        "--source", $Source,
        "--silver-path", "$lakeRoot/silver/$silverName",
        "--gold-path", "$lakeRoot/gold/features_$Source"
    )
}
Write-Host "HDFS pipeline OK ($Source)."
