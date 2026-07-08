# Docker janvier 2026 - volumes locaux (bind mounts) + pipeline + Grafana
param(
    [string]$Month = "2026-01",
    [switch]$SkipPipeline,
    [switch]$ResetVolumes
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

Write-Host "=== NYC Lakehouse Docker - $Month (volumes locaux) ===" -ForegroundColor Cyan

Write-Host "`n[1/5] Nettoyage anciens conteneurs et volumes Docker nommes..."
docker ps -aq --filter "name=nyc-" | ForEach-Object { docker stop $_ 2>$null; docker rm $_ 2>$null }
docker volume ls -q | Select-String "hdfs|mongo|grafana|prometheus|nyc" | ForEach-Object {
    Write-Host "  Suppression volume nomme: $_"
    docker volume rm $_ 2>$null
}

$volRoot = Join-Path $PWD "docker\volumes"
@("hdfs_namenode", "hdfs_datanode", "mongo", "prometheus", "grafana") | ForEach-Object {
    $p = Join-Path $volRoot $_
    if ($ResetVolumes -and (Test-Path $p)) {
        Write-Host "  Reset $p"
        Remove-Item $p -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $p | Out-Null
}

function Get-DirSizeGB($path) {
    if (-not (Test-Path $path)) { return 0 }
    return [math]::Round((Get-ChildItem $path -Recurse -File -EA SilentlyContinue | Measure-Object Length -Sum).Sum / 1GB, 3)
}

$vhdx = "$env:LOCALAPPDATA\Docker\wsl\data\ext4.vhdx"
if (Test-Path $vhdx) {
    Write-Host "  ext4.vhdx actuel: $([math]::Round((Get-Item $vhdx).Length/1GB, 2)) Go"
}
Write-Host "  Volumes locaux: $volRoot"

Write-Host "`n[2/5] Demarrage Docker (profil full)..."
& "$PSScriptRoot\run_docker.ps1" -Profile full -NoConfirm

Write-Host "Attente sante MongoDB + HDFS (60s)..."
Start-Sleep -Seconds 60

if (-not $SkipPipeline) {
    Write-Host "`n[3/5] Pipeline Docker mois $Month..."
    $start = Get-Date
    & "$PSScriptRoot\run_pipeline_docker.ps1" -Month $Month
    if ($LASTEXITCODE -ne 0) { throw "Pipeline Docker echoue" }
    $dur = [math]::Round(((Get-Date) - $start).TotalMinutes, 1)
    Write-Host "Pipeline termine en $dur min" -ForegroundColor Green
}
else {
    Write-Host "`n[3/5] Pipeline ignore (-SkipPipeline)"
    $dur = 0
}

Write-Host "`n[4/5] Compte rendu KPI..."
$env:MONGO_URI = "mongodb://localhost:27017"
.\.venv\Scripts\python.exe scripts\kpi_report.py

Write-Host "`n[5/5] Tailles stockage..."
@{
    "docker/volumes/hdfs_namenode" = Get-DirSizeGB "$volRoot\hdfs_namenode"
    "docker/volumes/hdfs_datanode"  = Get-DirSizeGB "$volRoot\hdfs_datanode"
    "docker/volumes/mongo"         = Get-DirSizeGB "$volRoot\mongo"
    "docker/volumes/prometheus"    = Get-DirSizeGB "$volRoot\prometheus"
    "docker/volumes/grafana"       = Get-DirSizeGB "$volRoot\grafana"
    "data/raw (sources)"           = Get-DirSizeGB "data\raw"
} | ForEach-Object { $_.GetEnumerator() } | ForEach-Object {
    Write-Host ("  {0,-30} {1,6} Go" -f $_.Key, $_.Value)
}
$hdfsTotal = (Get-DirSizeGB "$volRoot\hdfs_namenode") + (Get-DirSizeGB "$volRoot\hdfs_datanode")
Write-Host ("  HDFS total (local)            {0,6} Go" -f $hdfsTotal)

Write-Host "`n=== GRAFANA ===" -ForegroundColor Green
Write-Host "  URL       : http://localhost:3000 (admin / admin)"
Write-Host "  Dashboard : NYC Lakehouse > NYC Taxi Pipeline Monitoring"
Write-Host "  Spark     : http://localhost:8080"
Write-Host "  HDFS      : http://localhost:9870"
Write-Host "  Mongo UI  : http://localhost:8081 (admin / admin)"
Write-Host "  Prometheus: http://localhost:9090"
Write-Host "`nCompte rendu KPI : logs\compte_rendu_kpi.md" -ForegroundColor Cyan
