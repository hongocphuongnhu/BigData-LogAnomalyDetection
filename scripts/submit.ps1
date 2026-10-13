# Bao spark-submit len cluster standalone trong container spark-master.
#   .\scripts\submit.ps1 <job.py> [tham so cua job...]
# Connector Elasticsearch da co san trong image (docker/spark/Dockerfile).
param(
    [Parameter(Mandatory = $true, Position = 0)][string]$Job,
    [Parameter(ValueFromRemainingArguments = $true)][string[]]$JobArgs
)
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

# sh -c + "$@" giu nguyen tham so co dau cach khi chuyen vao container.
$inner = 'job="$0"; exec /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.sql.shuffle.partitions=16 --py-files /opt/spark/jobs/features_common.py "/opt/spark/jobs/$job" "$@"'
docker compose exec -T spark-master sh -c $inner $Job @JobArgs
exit $LASTEXITCODE
