# Benchmark pipeline sur un mois precis + rapport disque
param(
    [string]$Month = "2026-01",
    [switch]$Clean,
    [switch]$SkipRun
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

$env:HADOOP_HOME = "$PWD\tools\hadoop"
$env:HADOOP_CONF_DIR = "$PWD\tools\hadoop\etc\hadoop"

function Get-FolderSizeGB($relPath) {
    $p = Join-Path $PWD $relPath
    if (-not (Test-Path $p)) { return 0.0 }
    $sum = (Get-ChildItem $p -Recurse -File -EA SilentlyContinue | Measure-Object Length -Sum).Sum
    return [math]::Round($sum / 1GB, 3)
}

function Get-LayerBreakdown($base) {
    $p = Join-Path $PWD $base
    if (-not (Test-Path $p)) { return @() }
    Get-ChildItem $p -Directory -EA SilentlyContinue | ForEach-Object {
        $gb = (Get-ChildItem $_.FullName -Recurse -File -EA SilentlyContinue | Measure-Object Length -Sum).Sum / 1GB
        [PSCustomObject]@{ Path = "$base/$($_.Name)"; GB = [math]::Round($gb, 3) }
    } | Sort-Object GB -Descending
}

if ($Clean) {
    Write-Host "Nettoyage couches generees (raw conserve)..." -ForegroundColor Yellow
    foreach ($d in @("data\bronze", "data\silver", "data\gold", "data\lake")) {
        if (Test-Path $d) { Remove-Item $d -Recurse -Force }
    }
}

$before = @{
    bronze = Get-FolderSizeGB "data/bronze"
    silver = Get-FolderSizeGB "data/silver"
    gold   = Get-FolderSizeGB "data/gold"
    lake   = Get-FolderSizeGB "data/lake"
}

if (-not $SkipRun) {
    if (-not (Test-Path "tools\hadoop\bin\winutils.exe")) { .\scripts\setup_hadoop.ps1 }
    .\.venv\Scripts\Activate.ps1

    # LOCAL uniquement — pas de Docker
    $env:DOCKER_MODE = "false"
    $env:STORAGE_BACKEND = "local"
    Remove-Item Env:SPARK_MASTER_URL -EA SilentlyContinue

    $start = Get-Date
    Write-Host "Pipeline mois $Month (LOCAL, sans Docker)..." -ForegroundColor Cyan
    python -m src.pipeline.run_pipeline --layer all --month $Month --num-executors 2
    if ($LASTEXITCODE -ne 0) { throw "Pipeline echoue (code $LASTEXITCODE)" }
    $durationMin = [math]::Round(((Get-Date) - $start).TotalMinutes, 1)
} else {
    $durationMin = 0
}

$after = @{
    bronze = Get-FolderSizeGB "data/bronze"
    silver = Get-FolderSizeGB "data/silver"
    gold   = Get-FolderSizeGB "data/gold"
    lake   = Get-FolderSizeGB "data/lake"
}

$rawMonth = 0.0
foreach ($vt in @("yellow", "green", "fhv", "fhvhv")) {
    $f = Get-ChildItem "data\raw\$vt\*$Month*.parquet" -EA SilentlyContinue
    if ($f) { $rawMonth += ($f | Measure-Object Length -Sum).Sum }
}
$rawMonthGB = [math]::Round($rawMonth / 1GB, 3)

python scripts\storage_report.py $Month $durationMin
$reportPath = "logs\rapport_$($Month.Replace('-','')).md"
Write-Host "`nRapport complet: $reportPath" -ForegroundColor Green
Get-Content $reportPath
