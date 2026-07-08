# Synchronise les couches Medallion vers HDFS
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

if (-not (docker ps --filter "name=nyc-namenode" -q)) {
    Write-Host "HDFS non demarre. Lancez: .\scripts\run_docker.ps1 -Profile storage" -ForegroundColor Red
    exit 1
}

$layers = @("bronze", "silver", "gold", "lake")
foreach ($layer in $layers) {
    $local = "data\$layer"
    if (-not (Test-Path $local)) { continue }
    Write-Host "Sync $layer -> HDFS..."
    docker exec nyc-namenode bash -c "hdfs dfs -mkdir -p /data/$layer && hdfs dfs -put -f /data/$layer/* /data/$layer/" 2>$null
}
docker exec nyc-namenode hdfs dfs -ls -R /data 2>$null | Select-Object -First 30
Write-Host "Sync HDFS termine. UI: http://localhost:9870"
