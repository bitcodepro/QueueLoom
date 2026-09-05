$ErrorActionPreference = 'Stop'
$repository = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$project = Join-Path $repository 'dev/QueueLoom.Lab/QueueLoom.Lab.csproj'
Invoke-RestMethod http://localhost:5300/health -TimeoutSec 10 | Out-Null
dotnet build $project -c Release --configfile (Join-Path $repository 'NuGet.Config')
if ($LASTEXITCODE -ne 0) { throw 'Lab client build failed.' }
$labAssembly = Join-Path $repository 'dev/QueueLoom.Lab/bin/Release/net10.0/QueueLoom.Lab.dll'
dotnet $labAssembly seed
if ($LASTEXITCODE -ne 0) { throw 'Seeding failed.' }
dotnet $labAssembly install-profile
if ($LASTEXITCODE -ne 0) { throw 'Profile installation failed.' }
