# Demo complete : pipeline Docker + verification MongoDB
param(
    [switch]$Sample = $true
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

Write-Host "=== NYC Lakehouse Demo ===" -ForegroundColor Cyan

$spark = docker ps --filter "name=nyc-spark-client" -q
if (-not $spark) {
    Write-Host "Demarrage stack Docker (profil spark)..."
    & "$PSScriptRoot\run_docker.ps1" -Profile spark
}

$pipelineArgs = @("-Layer", "all")
if ($Sample) { $pipelineArgs += "-Sample" }
& "$PSScriptRoot\run_pipeline_docker.ps1" @pipelineArgs

Write-Host "`n=== Verification ===" -ForegroundColor Green
.\.venv\Scripts\Activate.ps1
python scripts\status.py
python scripts\query_kpis.py
python scripts\query_lake.py

Write-Host "`nDemo terminee." -ForegroundColor Green
