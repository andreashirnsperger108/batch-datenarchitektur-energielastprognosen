[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$examplePath = Join-Path $projectRoot '.env.example'
$targetPath = Join-Path $projectRoot '.env'

if (Test-Path -LiteralPath $targetPath) {
    Write-Host '.env already exists; no values were changed.'
    exit 0
}

$content = Get-Content -Raw -LiteralPath $examplePath
$postgresSecret = [Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(24))
$airflowSecret = [Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(24))
$content = $content.Replace('POSTGRES_PASSWORD=change-me-before-sharing', "POSTGRES_PASSWORD=$postgresSecret")
$content = $content.Replace('AIRFLOW_ADMIN_PASSWORD=change-me-before-sharing', "AIRFLOW_ADMIN_PASSWORD=$airflowSecret")
[IO.File]::WriteAllText($targetPath, $content, [Text.UTF8Encoding]::new($false))
Write-Host 'Created an ignored .env file with random local passwords.'

