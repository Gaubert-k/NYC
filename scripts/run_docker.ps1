# Lance la stack Docker NYC Lakehouse par profil
param(
    [ValidateSet("mongodb", "storage", "spark", "monitoring", "ui", "data", "full")]
    [string]$Profile = "full",
    [switch]$NoConfirm
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

function Test-DockerReady {
    docker info 2>$null | Out-Null
    return $LASTEXITCODE -eq 0
}

if (-not (Test-DockerReady)) {
    Write-Host "Demarrage de Docker Desktop..."
    Start-Process "${env:ProgramFiles}\Docker\Docker\Docker Desktop.exe" -ErrorAction SilentlyContinue
    for ($i = 0; $i -lt 60; $i++) {
        if (Test-DockerReady) { break }
        Start-Sleep -Seconds 5
    }
    if (-not (Test-DockerReady)) { throw "Docker Desktop non disponible" }
}

$ram = @{
    mongodb   = "~500 Mo"
    storage   = "~2 Go (HDFS)"
    spark     = "~5 Go (cluster + client)"
    monitoring = "~1.5 Go"
    data      = "~3 Go (MongoDB + HDFS)"
    full      = "~10-14 Go RAM"
}

Write-Host "Profil: $Profile (RAM estimee: $($ram[$Profile]))" -ForegroundColor Cyan
if (-not $NoConfirm -and $Profile -in @("full", "spark", "storage")) {
    Write-Host "Appuyez sur Entree pour continuer ou Ctrl+C pour annuler..."
    Read-Host
}

$envFile = if (Test-Path "docker\.env") { "docker\.env" } else { "docker\.env.example" }
$compose = "docker compose -f docker\docker-compose.yml --env-file $envFile"

switch ($Profile) {
    "mongodb"   { Invoke-Expression "$compose --profile mongodb up -d" }
    "storage"   { Invoke-Expression "$compose --profile storage up -d" }
    "spark"     { Invoke-Expression "$compose --profile storage --profile spark --profile mongodb up -d" }
    "monitoring"{ Invoke-Expression "$compose --profile monitoring up -d" }
    "ui"        { Invoke-Expression "$compose --profile mongodb --profile ui up -d" }
    "data"      { Invoke-Expression "$compose --profile mongodb --profile storage up -d" }
    "full"      { Invoke-Expression "$compose --profile full up -d" }
}

Start-Sleep -Seconds 3
docker ps --filter "network=nyc-data-net" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

if ($Profile -in @("storage", "full", "data", "spark")) {
    Write-Host "`nInitialisation HDFS (dossiers Medallion)..."
    Start-Sleep -Seconds 15
    if (docker ps --filter "name=nyc-namenode" --filter "status=running" -q) {
        docker exec nyc-namenode bash -c "hdfs dfs -mkdir -p /data/bronze /data/silver /data/gold /data/lake /data/raw /spark-logs && hdfs dfs -chmod -R 777 /data /spark-logs && hdfs dfs -ls /data" 2>$null
    } else {
        Write-Host "  namenode non demarre, init HDFS ignoree" -ForegroundColor Yellow
    }
}

Write-Host "`n=== URLs ===" -ForegroundColor Green
@{
    "HDFS NameNode UI" = "http://localhost:9870"
    "Spark Master UI"  = "http://localhost:8080"
    "MongoDB"          = "mongodb://localhost:27017"
    "Mongo Express"    = "http://localhost:8081 (admin/admin)"
    "Grafana"          = "http://localhost:3000 (admin/admin)"
    "Prometheus"       = "http://localhost:9090"
    "cAdvisor"         = "http://localhost:8082"
} | ForEach-Object { $_.GetEnumerator() } | ForEach-Object { Write-Host "  $($_.Key): $($_.Value)" }

Write-Host "`nPipeline dans Spark client:"
Write-Host "  .\scripts\run_pipeline_docker.ps1 --sample"
