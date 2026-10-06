param([Parameter(Mandatory)] [ValidateSet('win-x64', 'linux-x64', 'osx-arm64', 'osx-x64')] [string] $Rid)
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'smoke-processes.ps1')
$source = (Resolve-Path -LiteralPath "artifacts/package-$Rid").Path
$root = [IO.Path]::GetFullPath((Join-Path ([IO.Path]::GetTempPath()) ('QueueLoom stable smoke ' + [Guid]::NewGuid().ToString('N'))))
$installation = Join-Path $root 'installation'
$candidate = Join-Path $root 'candidate'
$data = Join-Path $root 'user data'
$relative = if ($Rid -eq 'win-x64') { 'QueueLoom.exe' } elseif ($Rid.StartsWith('osx-')) { 'QueueLoom.app/Contents/MacOS/QueueLoom' } else { 'QueueLoom' }
$executable = Join-Path $installation $relative
$fixture = (Resolve-Path -LiteralPath ('tests/QueueLoom.UpdateFixture/bin/Release/net10.0/QueueLoom.UpdateFixture' + $(if ($IsWindows) { '.exe' }))).Path
$store = Join-Path $installation 'QueueLoom.versions'
function Digest([string] $Path) { (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant() }
function Copy-Package([string] $Destination) {
    New-Item -ItemType Directory -Path $Destination | Out-Null
    Get-ChildItem -LiteralPath $source -Force | Copy-Item -Destination $Destination -Recurse
}
function Run-Mcp {
    $start = [Diagnostics.ProcessStartInfo]::new($executable)
    $start.UseShellExecute = $false
    $start.RedirectStandardInput = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.CreateNoWindow = $true
    $start.ArgumentList.Add('--mcp')
    $start.ArgumentList.Add('--read-only')
    $start.Environment['QUEUELOOM_DATA_DIRECTORY'] = $data
    $start.Environment['QUEUELOOM_BACKUP_DIRECTORY'] = Join-Path $root 'backups'
    $start.Environment['DOTNET_BUNDLE_EXTRACT_BASE_DIR'] = Join-Path $root 'bundle cache'
    $process = [Diagnostics.Process]::Start($start)
    if ($null -eq $process) { throw 'Stable launcher did not start' }
    try {
        $errors = $process.StandardError.ReadToEndAsync()
        # Send immediately: the launcher must retain this first request until candidate readiness.
        $process.StandardInput.WriteLine('{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"stable-update-smoke","version":"1"}}}')
        $line = $process.StandardOutput.ReadLineAsync().WaitAsync([TimeSpan]::FromSeconds(60)).GetAwaiter().GetResult()
        if ($null -eq $line) { throw 'The packaged payload exited before replying' }
        $response = $line | ConvertFrom-Json
        if ($response.id -ne 1 -or $response.result.serverInfo.name -ne 'QueueLoom') { throw 'Stable package MCP initialization failed' }
    } finally {
        $process.StandardInput.Close()
        $payloads = @(Get-OwnedSmokeProcesses $installation $process.Id)
        if (-not $process.WaitForExit(15000)) { $process.Kill($true); $process.WaitForExit() }
        foreach ($payload in $payloads) {
            if (-not $payload.HasExited) { $payload.Kill($true); $payload.WaitForExit() }
            $payload.Dispose()
        }
        $process.Dispose()
    }
}
try {
    New-Item -ItemType Directory -Path $root | Out-Null
    Copy-Package $installation
    Copy-Package $candidate
    [IO.File]::WriteAllText((Join-Path $installation 'user-notes.txt'), 'keep')
    $entryHash = Digest $executable
    Run-Mcp
    $descriptorPath = if ($Rid.StartsWith('osx-')) { Join-Path $candidate 'QueueLoom.app/Contents/Resources/QueueLoom.bootstrap.json' } else { Join-Path $candidate 'QueueLoom.bootstrap.json' }
    $descriptor = Get-Content -LiteralPath $descriptorPath -Raw | ConvertFrom-Json
    $initialId = $descriptor.Id
    $id = [Guid]::NewGuid().ToString('N')
    $content = if ($Rid.StartsWith('osx-')) { Join-Path $candidate 'QueueLoom.app/Contents/Resources/initial' } else { Join-Path $root 'candidate content' }
    if (-not $Rid.StartsWith('osx-')) { [IO.Compression.ZipFile]::ExtractToDirectory((Join-Path $candidate 'QueueLoom.bootstrap.zip'), $content) }
    $manifestPath = Join-Path $content 'manifest.json'
    $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
    $manifest.Id = $id
    [IO.File]::WriteAllText($manifestPath, ($manifest | ConvertTo-Json -Depth 8))
    $descriptor.Id = $id
    $descriptor.ManifestSha256 = Digest $manifestPath
    if (-not $Rid.StartsWith('osx-')) {
        $zip = Join-Path $candidate 'QueueLoom.bootstrap.zip'
        Remove-Item -LiteralPath $zip
        [IO.Compression.ZipFile]::CreateFromDirectory($content, $zip)
        $descriptor.ArchiveSha256 = Digest $zip
    }
    [IO.File]::WriteAllText($descriptorPath, ($descriptor | ConvertTo-Json))
    & $fixture --stage-version $executable $candidate
    if ($LASTEXITCODE -ne 0) { throw 'Staging the packaged update failed' }
    Run-Mcp
    $stateFile = Get-ChildItem -LiteralPath (Join-Path $store 'state') -Filter '*.json' | Sort-Object Name | Select-Object -Last 1
    $state = (Get-Content -LiteralPath $stateFile.FullName -Raw | ConvertFrom-Json).State
    if ($state.Active.Id -ne $id -or $state.Pending) { throw 'The packaged candidate did not confirm startup' }
    # Corrupt only this test's committed candidate: a new stable launch must recover the verified bootstrap.
    $payloadExe = Join-Path (Join-Path $store "versions/$id/payload") $(if ($Rid.StartsWith('osx-')) { 'QueueLoom.app/Contents/MacOS/QueueLoom' } elseif ($IsWindows) { 'QueueLoom.exe' } else { 'QueueLoom' })
    [IO.File]::WriteAllText($payloadExe, 'corrupt owned candidate')
    Run-Mcp
    $stateFile = Get-ChildItem -LiteralPath (Join-Path $store 'state') -Filter '*.json' | Sort-Object Name | Select-Object -Last 1
    if ((Get-Content -LiteralPath $stateFile.FullName -Raw | ConvertFrom-Json).State.Active.Id -ne $initialId) { throw 'Verified fallback was not selected' }
    if ((Digest $executable) -ne $entryHash -or [IO.File]::ReadAllText((Join-Path $installation 'user-notes.txt')) -ne 'keep') { throw 'Stable entry or user files changed' }
    Write-Host "PASS $Rid stable update: native fresh MCP, staged activation, acknowledgement, corrupt-payload recovery, unchanged launch entry and user file."
} finally {
    # The absolute random root belongs only to this invocation. Never touch a real installation.
    if (-not $root.StartsWith([IO.Path]::GetFullPath([IO.Path]::GetTempPath()), [StringComparison]::OrdinalIgnoreCase)) { throw 'Smoke root escaped the temporary workspace' }
    if (Test-Path -LiteralPath $root) { [IO.Directory]::Delete($root, $true) }
}
