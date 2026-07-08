# Execute le pipeline PySpark dans le conteneur spark-client (cluster Spark)
param(
    [string]$Layer = "all",
    [switch]$Sample,
    [switch]$Light,
    [string]$VehicleType = "",
    [string]$Month = ""
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

$running = docker ps --filter "name=nyc-spark-client" --filter "status=running" -q
if (-not $running) {
    Write-Host "spark-client non demarre. Lancez: .\scripts\run_docker.ps1 -Profile spark" -ForegroundColor Red
    exit 1
}

if (docker ps --filter "name=nyc-namenode" -q) {
    Write-Host "Verification repertoires HDFS (spark-logs)..."
    docker exec nyc-namenode bash -c "hdfs dfs -mkdir -p /data/bronze /data/silver /data/gold /data/lake /data/raw /spark-logs && hdfs dfs -chmod -R 777 /data /spark-logs" *> $null
}

Write-Host "Installation dependances Python dans spark-client..."
$prevEap = $ErrorActionPreference
$ErrorActionPreference = "Continue"
docker exec nyc-spark-client pip install -q -r /app/requirements.txt *> $null
$ErrorActionPreference = $prevEap

$pyArgs = @("--layer", $Layer)
if ($Sample) { $pyArgs += "--sample" }
if ($Month) { $pyArgs += @("--month", $Month) }
if ($Light) { $pyArgs += "--light" }
if ($VehicleType) { $pyArgs += @("--vehicle-type", $VehicleType) }

Write-Host "Execution cluster Spark: spark-submit $($pyArgs -join ' ')"
docker exec -w /app `
    -e DOCKER_MODE=true `
    -e STORAGE_BACKEND=hdfs `
    -e HADOOP_USER_NAME=spark `
    -e MONGO_URI=mongodb://mongodb:27017 `
    -e SPARK_MASTER_URL=spark://spark-master:7077 `
    -e PROJECT_ROOT=/app `
    -e PYTHONPATH=/app `
    -e SAMPLE_MODE=$(if ($Sample -or $Month) { 'true' } else { 'false' }) `
    $(if ($Sample -or $Month) { '-e MAX_MONTHS_PER_TYPE=1' }) `
    $(if ($Month) { "-e SAMPLE_MONTH=$Month" }) `
    -e LIGHT_MODE=$(if ($Light) { 'true' } else { 'false' }) `
    nyc-spark-client /opt/bitnami/spark/bin/spark-submit `
        --master spark://spark-master:7077 `
        --driver-memory 1g `
        --executor-memory 1g `
        --executor-cores 1 `
        --conf spark.executor.instances=2 `
        --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 `
        --conf spark.executorEnv.HADOOP_USER_NAME=spark `
        --conf spark.driver.extraPythonPath=/app `
        --conf spark.executorEnv.PYTHONPATH=/app `
        src/pipeline/run_pipeline.py @pyArgs
