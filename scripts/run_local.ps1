# Pipeline local sans Docker
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

$env:HADOOP_HOME = "$PWD\tools\hadoop"
$env:HADOOP_CONF_DIR = "$PWD\tools\hadoop\etc\hadoop"

if (-not (Test-Path ".venv")) {
    python -m venv .venv
}

if (-not (Test-Path "tools\hadoop\bin\winutils.exe")) {
    .\scripts\setup_hadoop.ps1
}

.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt -q

Write-Host "Lancement pipeline SAMPLE_MODE (2 mois par type)..."
python -m src.pipeline.run_pipeline --layer all --sample --num-executors 2

Write-Host "`nRequetes KPI exemple:"
python scripts\query_kpis.py
