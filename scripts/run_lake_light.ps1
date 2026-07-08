# Mode ultra-léger (~1 Go RAM) — utilise Silver existant si disponible
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

$env:HADOOP_HOME = "$PWD\tools\hadoop"
$env:HADOOP_CONF_DIR = "$PWD\tools\hadoop\etc\hadoop"
$env:LIGHT_MODE = "true"
$env:SAMPLE_MODE = "true"
$env:MAX_MONTHS_PER_TYPE = "1"
$env:SPARK_DRIVER_MEMORY = "512m"
$env:SPARK_EXECUTOR_MEMORY = "512m"
$env:SPARK_NUM_EXECUTORS = "1"

.\.venv\Scripts\Activate.ps1

# Si Silver déjà généré, ne lancer que la couche Lake (le plus intéressant)
$silverExists = Test-Path "data\silver\trips_unified"
if ($silverExists) {
    Write-Host "Silver détecté — lancement couche Lake uniquement (géo, ML, anomalies)..."
    python -m src.pipeline.run_pipeline --layer lake --light
} else {
    Write-Host "Premier run — pipeline complet en mode léger (green uniquement implicite)..."
    python -m src.pipeline.run_pipeline --layer all --light --vehicle-type green
}

Write-Host "`nRésultats Lake :"
Get-ChildItem data\lake -Recurse -Directory | Select-Object FullName
