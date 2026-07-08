# Arrete la stack Docker
param(
    [switch]$Volumes
)

Set-Location $PSScriptRoot\..
$envFile = if (Test-Path "docker\.env") { "docker\.env" } else { "docker\.env.example" }

if ($Volumes) {
    docker compose -f docker\docker-compose.yml --env-file $envFile --profile full down -v
} else {
    docker compose -f docker\docker-compose.yml --env-file $envFile --profile full down
}
Write-Host "Stack arretee."
