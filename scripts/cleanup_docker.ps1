# Nettoyage Docker + diagnostic disque (apres crash ou avant soutenance)
$ErrorActionPreference = "Continue"
Set-Location $PSScriptRoot\..

Write-Host "=== DIAGNOSTIC DISQUE NYC LAKEHOUSE ===" -ForegroundColor Cyan

# Taille projet local
$folders = @{
    "data/raw"   = "Sources Parquet (normal ~7 Go)"
    "data/silver"= "Silver local (sample ~0.5 Go)"
    "data/bronze"= "Bronze local"
    "logs"       = "Logs pipeline"
    ".venv"      = "Python venv"
}
foreach ($rel in $folders.Keys) {
    $p = Join-Path $PWD $rel
    if (Test-Path $p) {
        $gb = (Get-ChildItem $p -Recurse -File -ErrorAction SilentlyContinue | Measure-Object Length -Sum).Sum / 1GB
        Write-Host ("  {0,-14} {1,6:N2} Go  ({2})" -f $rel, $gb, $folders[$rel])
    }
}

# Docker WSL disk (Windows)
$wslDisk = "$env:LOCALAPPDATA\Docker\wsl\disk\docker_data.vhdx"
$wslData = "$env:LOCALAPPDATA\Docker\wsl\data\ext4.vhdx"
foreach ($f in @($wslDisk, $wslData)) {
    if (Test-Path $f) {
        $gb = (Get-Item $f).Length / 1GB
        Write-Host ("  Docker WSL     {0,6:N2} Go  ({1})" -f $gb, (Split-Path $f -Leaf))
    }
}

Write-Host "`n=== POURQUOI 150 Go ? ===" -ForegroundColor Yellow
Write-Host @"
  6,8 Go sources  ->  copiees dans HDFS (Bronze)
  306M lignes Silver en Parquet HDFS  ->  30-80 Go estime
  Lake geospatial (shuffles Spark)     ->  10-30 Go temporaires
  Spark event logs (/spark-logs)       ->  plusieurs Go (12h de run)
  = tout stocke dans le disque virtuel Docker (ext4.vhdx)
  Les 6,8 Go sources sur ton disque Windows sont EN PLUS, pas a la place.
"@

# Docker cleanup si daemon actif
docker info 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "`nDocker arrete - rien a tuer." -ForegroundColor Green
    Write-Host "Pour liberer lespace WSL restant: Docker Desktop > Settings > Resources > Disk image location"
    exit 0
}

Write-Host "`n=== NETTOYAGE DOCKER ===" -ForegroundColor Cyan
docker ps -a --filter "name=nyc-" --format "{{.Names}} {{.Status}}"
Write-Host "Arret conteneurs NYC..."
docker ps -aq --filter "name=nyc-" | ForEach-Object { docker stop $_ 2>$null; docker rm $_ 2>$null }
Write-Host "Suppression volumes nommes Docker (HDFS, MongoDB, Grafana)..."
docker volume ls -q | Select-String "hdfs|mongo|grafana|prometheus|minio|nyc" | ForEach-Object { docker volume rm $_ 2>$null }
Write-Host "Volumes locaux (bind mounts): docker\volumes\ — supprimer ce dossier pour liberer l'espace HDFS"
Write-Host "Prune systeme Docker..."
docker system prune -f --volumes 2>$null
docker system df
Write-Host "`nNettoyage termine." -ForegroundColor Green
