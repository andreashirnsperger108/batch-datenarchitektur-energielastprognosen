[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$examplePath = Join-Path $projectRoot '.env.example'
$targetPath = Join-Path $projectRoot '.env'

function New-RandomSecret {
    [Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(64))
}

if (Test-Path -LiteralPath $targetPath) {
    $content = Get-Content -Raw -LiteralPath $targetPath
    $changed = $false
    foreach ($entry in @(
        @{ Name = 'AIRFLOW_API_SECRET_KEY'; Value = (New-RandomSecret) },
        @{ Name = 'AIRFLOW_JWT_SECRET'; Value = (New-RandomSecret) }
    )) {
        if ($content -notmatch "(?m)^$($entry.Name)=") {
            if ($content.Length -gt 0 -and -not $content.EndsWith("`n")) {
                $content += "`r`n"
            }
            $content += "$($entry.Name)=$($entry.Value)`r`n"
            $changed = $true
        }
    }
    if ($changed) {
        [IO.File]::WriteAllText($targetPath, $content, [Text.UTF8Encoding]::new($false))
        Write-Host 'Added missing ignored Airflow API/JWT secrets to .env.'
    } else {
        Write-Host '.env already contains the Airflow API/JWT secrets; no values were changed.'
    }
    exit 0
}

$content = Get-Content -Raw -LiteralPath $examplePath
$postgresSecret = [Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(24))
$airflowSecret = [Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(24))
$airflowApiSecret = New-RandomSecret
$airflowJwtSecret = New-RandomSecret
$content = $content.Replace('POSTGRES_PASSWORD=change-me-before-sharing', "POSTGRES_PASSWORD=$postgresSecret")
$content = $content.Replace('AIRFLOW_ADMIN_PASSWORD=change-me-before-sharing', "AIRFLOW_ADMIN_PASSWORD=$airflowSecret")
$content = $content.Replace('AIRFLOW_API_SECRET_KEY=change-me-before-sharing', "AIRFLOW_API_SECRET_KEY=$airflowApiSecret")
$content = $content.Replace('AIRFLOW_JWT_SECRET=change-me-before-sharing', "AIRFLOW_JWT_SECRET=$airflowJwtSecret")
[IO.File]::WriteAllText($targetPath, $content, [Text.UTF8Encoding]::new($false))
Write-Host 'Created an ignored .env file with random local passwords and Airflow API/JWT secrets.'
