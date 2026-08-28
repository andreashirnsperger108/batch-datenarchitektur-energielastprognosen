[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    docker compose config --quiet
    docker build --target test -f docker/ingestion/Dockerfile -t power-pipeline-tests .
    docker run --rm power-pipeline-tests
} finally {
    Pop-Location
}

