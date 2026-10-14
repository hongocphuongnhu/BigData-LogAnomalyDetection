param([switch]$Serve, [switch]$Batch, [switch]$Bootstrap, [switch]$Hdfs)
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
$runArgs = @("-B", "scripts/run_all.py")
if ($Serve) { $runArgs += "--serve" }
if ($Batch) { $runArgs += "--batch" }
if ($Bootstrap) { $runArgs += "--bootstrap" }
if ($Hdfs) { $runArgs += "--hdfs" }
python @runArgs
exit $LASTEXITCODE
