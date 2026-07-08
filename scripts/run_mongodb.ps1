# Lance uniquement MongoDB (~500 Mo RAM) - pas toute la stack
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

function Test-DockerReady {
    docker info 2>$null | Out-Null
    return $LASTEXITCODE -eq 0
}

if (-not (Test-DockerReady)) {
    Write-Host "Docker Desktop n'est pas demarre. Tentative de lancement..."
    $dockerDesktop = "${env:ProgramFiles}\Docker\Docker\Docker Desktop.exe"
    if (Test-Path $dockerDesktop) {
        Start-Process $dockerDesktop
        $retries = 0
        while (-not (Test-DockerReady) -and $retries -lt 60) {
            Start-Sleep -Seconds 5
            $retries++
            Write-Host "  Attente Docker... ($($retries * 5)s)"
        }
    }
    if (-not (Test-DockerReady)) {
        Write-Host "ERREUR: Demarrez Docker Desktop manuellement puis relancez ce script." -ForegroundColor Red
        exit 1
    }
}

Write-Host "Demarrage MongoDB seul..."
docker compose -f docker\docker-compose.yml --env-file docker\.env.example --profile mongodb up -d

Write-Host "`nMongoDB pret:"
Write-Host "  URI: mongodb://localhost:27017"
Write-Host "  Database: nyc_taxi_warehouse"
Write-Host "`nCharger les KPIs Gold:"
Write-Host "  python -m src.pipeline.run_pipeline --layer gold"
