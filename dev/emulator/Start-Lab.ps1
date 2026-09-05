$ErrorActionPreference = 'Stop'
$dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
$dockerPath = if ($dockerCommand) { $dockerCommand.Source } else { Join-Path $env:LOCALAPPDATA 'Programs/DockerDesktop/resources/bin/docker.exe' }
if (!(Test-Path -LiteralPath $dockerPath)) { throw 'Docker Desktop CLI was not found.' }
$envFile = Join-Path $PSScriptRoot '.env'
if (!(Test-Path -LiteralPath $envFile)) {
    $password = 'Ql!' + [Convert]::ToHexString([System.Security.Cryptography.RandomNumberGenerator]::GetBytes(24)) + 'a9'
    Set-Content -LiteralPath $envFile -Value "MSSQL_SA_PASSWORD=$password" -Encoding utf8
}
& $dockerPath compose --env-file $envFile -f (Join-Path $PSScriptRoot 'compose.yaml') up -d
if ($LASTEXITCODE -ne 0) { throw 'Docker Compose failed.' }
