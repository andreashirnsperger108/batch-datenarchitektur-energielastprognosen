[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    docker build --target runtime -f docker/ingestion/Dockerfile -t power-forecast-pipeline-ingestion-api .
    docker build -f docker/airflow/Dockerfile -t power-forecast-pipeline-airflow .
    docker build -f docker/spark-driver/Dockerfile -t power-forecast-pipeline-spark-driver-api .
} finally {
    Pop-Location
}
