# Preparation soutenance — demo reproductible (~15 min)
param(
    [ValidateSet("local", "docker")]
    [string]$Mode = "local"
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

Write-Host "=== PREPARATION SOUTENANCE NYC LAKEHOUSE ===" -ForegroundColor Cyan
Write-Host "Mode: $Mode`n"

if (-not (Test-Path ".venv")) {
    python -m venv .venv
}
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt -q

if (-not (Test-Path "tools\hadoop\bin\winutils.exe")) {
    .\scripts\setup_hadoop.ps1
}

$env:HADOOP_HOME = "$PWD\tools\hadoop"
$env:HADOOP_CONF_DIR = "$PWD\tools\hadoop\etc\hadoop"

if ($Mode -eq "docker") {
    docker info 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Docker non demarre — bascule en mode local" -ForegroundColor Yellow
        $Mode = "local"
    } else {
        & "$PSScriptRoot\run_docker.ps1" -Profile spark -NoConfirm
        & "$PSScriptRoot\run_pipeline_docker.ps1" -Layer all -Sample
    }
}

if ($Mode -eq "local") {
    Write-Host "Pipeline local SAMPLE (~15 min)..."
    python -m src.pipeline.run_pipeline --layer all --sample
}

Write-Host "`n=== VERIFICATION ===" -ForegroundColor Green
python scripts\status.py
python scripts\query_kpis.py
python scripts\query_lake.py
python scripts\show_lake_outputs.py

Write-Host "`n=== PRET POUR VENDREDI ===" -ForegroundColor Green
Write-Host "Commandes demo live:"
Write-Host "  python scripts\status.py"
Write-Host "  python scripts\query_kpis.py"
Write-Host "  python scripts\query_lake.py"
if ($Mode -eq "docker") {
    Write-Host "  http://localhost:8081  Mongo Express (admin/admin)"
    Write-Host "  http://localhost:3000   Grafana (admin/admin)"
    Write-Host "  http://localhost:8080   Spark Master"
}
