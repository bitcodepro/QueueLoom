param([Parameter(Mandatory)] [string] $Installer)
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'smoke-processes.ps1')
# Run only on an isolated Windows CI image; never replace a user's registered install.
$key = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\QueueLoom_is1'
if (Test-Path -LiteralPath $key) { throw 'Installer smoke requires a fresh Windows user' }
$root = Join-Path ([IO.Path]::GetTempPath()) ("QueueLoom installer smoke " + [Guid]::NewGuid().ToString('N'))
$installed = Join-Path $root 'program'
$data = Join-Path $root 'user data'
$earlier = Join-Path $root 'earlier'
$fixtureContent = Join-Path $root 'payload fixture'
$source = (Resolve-Path -LiteralPath 'artifacts/package-win-x64').Path
$installer = (Resolve-Path -LiteralPath $Installer).Path
$executable = Join-Path $installed 'QueueLoom.exe'
$env:QUEUELOOM_DATA_DIRECTORY = $data
$env:DOTNET_BUNDLE_EXTRACT_BASE_DIR = Join-Path $root 'bundle cache'
Remove-Item Env:\QUEUELOOM_BACKUP_DIRECTORY -ErrorAction SilentlyContinue
function Digest([string] $Path) { (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant() }
function Invoke-Setup([string] $Path, [string] $Log) {
    $arguments = @('/VERYSILENT', '/SUPPRESSMSGBOXES', '/NORESTART', '/NOICONS', "/DIR=$installed", "/LOG=$Log")
    Invoke-Program $Path $arguments
}
function Invoke-Program([string] $Path, [string[]] $Arguments) {
    $start = [Diagnostics.ProcessStartInfo]::new($Path)
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    foreach ($argument in $Arguments) { $start.ArgumentList.Add($argument) }
    $process = [Diagnostics.Process]::Start($start)
    try {
        if (-not $process.WaitForExit(300000)) {
            $process.Kill($true); $process.WaitForExit()
            throw "Timed out running $Path"
        }
        return $process.ExitCode
    } finally { $process.Dispose() }
}
function Assert-Mcp {
    $start = [Diagnostics.ProcessStartInfo]::new($executable)
    $start.UseShellExecute = $false
    $start.RedirectStandardInput = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.CreateNoWindow = $true
    $start.ArgumentList.Add('--mcp')
    $start.ArgumentList.Add('--read-only')
    $process = [Diagnostics.Process]::Start($start)
    try {
        $errors = $process.StandardError.ReadToEndAsync()
        $process.StandardInput.WriteLine('{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"installer-smoke","version":"1"}}}')
        $line = $process.StandardOutput.ReadLineAsync().WaitAsync([TimeSpan]::FromSeconds(60)).GetAwaiter().GetResult()
        if ($null -eq $line) { throw "The installed payload exited before replying: $($errors.GetAwaiter().GetResult())" }
        if (($line | ConvertFrom-Json).result.serverInfo.name -ne 'QueueLoom') { throw 'Installed MCP initialization failed' }
    } finally {
        $process.StandardInput.Close()
        $payloads = @(Get-OwnedSmokeProcesses $installed $process.Id)
        if (-not $process.WaitForExit(15000)) { $process.Kill($true); $process.WaitForExit() }
        foreach ($payload in $payloads) {
            if (-not $payload.HasExited) { $payload.Kill($true); $payload.WaitForExit() }
            $payload.Dispose()
        }
        $process.Dispose()
    }
}
function Assert-ACTIVE([string] $ExpectedId) {
    $state = Get-ChildItem -LiteralPath (Join-Path $installed 'QueueLoom.versions/state') -Filter '*.json' | Sort-Object Name | Select-Object -Last 1
    $record = (Get-Content -LiteralPath $state.FullName -Raw | ConvertFrom-Json).State
    if ($record.Active.Id -ne $ExpectedId -or $record.Pending) { throw 'The installer candidate did not become the confirmed active payload' }
}

try {
    New-Item -ItemType Directory -Path $earlier, $data -Force | Out-Null
    Get-ChildItem -LiteralPath $source | Copy-Item -Destination $earlier -Recurse
    $oldArchive = Join-Path $earlier 'QueueLoom.bootstrap.zip'
    [IO.Compression.ZipFile]::ExtractToDirectory($oldArchive, $fixtureContent)
    $manifestPath = Join-Path $fixtureContent 'manifest.json'
    $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
    # Use the same executable with an older manifest/ID to verify exactly which payload won,
    # without fetching or trusting a historical artifact from outside this run.
    $manifest.Version = '0.0.0-0'
    $manifest.Id = [Guid]::NewGuid().ToString('N')
    [IO.File]::WriteAllText($manifestPath, ($manifest | ConvertTo-Json -Depth 8))
    [IO.File]::Delete($oldArchive)
    [IO.Compression.ZipFile]::CreateFromDirectory($fixtureContent, $oldArchive)
    $oldDescriptorPath = Join-Path $earlier 'QueueLoom.bootstrap.json'
    $oldDescriptor = Get-Content -LiteralPath $oldDescriptorPath -Raw | ConvertFrom-Json
    $oldDescriptor.Id = $manifest.Id
    $oldDescriptor.ManifestSha256 = Digest $manifestPath
    $oldDescriptor.ArchiveSha256 = Digest $oldArchive
    [IO.File]::WriteAllText($oldDescriptorPath, ($oldDescriptor | ConvertTo-Json))
    & (Join-Path $PSScriptRoot 'build-windows-installer.ps1') -Version '0.0.0-0' -InputDirectory $earlier -OutputDirectory $root
    $oldInstaller = Join-Path $root 'QueueLoom-0.0.0-0-win-x64-setup.exe'
    $freshLog = Join-Path $root 'fresh.log'
    if ((Invoke-Setup $oldInstaller $freshLog) -ne 0 -or -not (Test-Path -LiteralPath $key)) { throw 'Initial setup failed' }
    foreach ($file in 'QueueLoom.exe', 'QueueLoom.bootstrap.json', 'QueueLoom.bootstrap.zip') {
        if ((Digest (Join-Path $installed $file)) -ne (Digest (Join-Path $earlier $file))) { throw "Installer modified $file" }
    }
    Assert-Mcp
    $backup = Join-Path $installed 'backups/keep.txt'
    New-Item -ItemType Directory -Path (Split-Path $backup) -Force | Out-Null
    [IO.File]::WriteAllText($backup, 'keep backup')
    $settingsMarker = Join-Path $data 'keep.txt'
    [IO.File]::WriteAllText($settingsMarker, 'keep settings')
    if ((Invoke-Setup $installer (Join-Path $root 'upgrade.log')) -ne 0) { throw 'Upgrade setup failed' }
    Assert-Mcp
    $expected = (Get-Content -LiteralPath (Join-Path $source 'QueueLoom.bootstrap.json') -Raw | ConvertFrom-Json).Id
    Assert-ACTIVE $expected
    foreach ($file in 'QueueLoom.exe', 'QueueLoom.bootstrap.json', 'QueueLoom.bootstrap.zip') {
        if ((Digest (Join-Path $installed $file)) -ne (Digest (Join-Path $earlier $file))) { throw "Upgrade replaced $file" }
    }
    if ((Invoke-Setup $installer (Join-Path $root 'same.log')) -ne 0) { throw 'Equal version was not idempotent' }
    if ((Invoke-Setup $oldInstaller (Join-Path $root 'downgrade.log')) -eq 0) { throw 'Older Setup unexpectedly succeeded' }
    Assert-ACTIVE $expected
    $reg = Get-ItemProperty -LiteralPath $key
    $uninstaller = $reg.UninstallString.Trim('"')
    if ([IO.Path]::GetDirectoryName($uninstaller) -ne $installed -or
        (Invoke-Program $uninstaller @('/VERYSILENT', '/SUPPRESSMSGBOXES', '/NORESTART')) -ne 0) {
        throw 'Uninstall failed'
    }
    if ((Test-Path -LiteralPath $key) -or (Test-Path -LiteralPath $executable)) { throw 'Uninstall left its registered launcher behind' }
    if ([IO.File]::ReadAllText($settingsMarker) -ne 'keep settings' -or [IO.File]::ReadAllText($backup) -ne 'keep backup') {
        throw 'Uninstall changed settings or backups'
    }
    if ((Invoke-Setup $installer (Join-Path $root 'reinstall.log')) -ne 0) { throw 'Reinstall failed' }
    Assert-Mcp
    Assert-ACTIVE $expected
    Write-Host 'PASS fresh install, manual activation, unchanged launcher/bootstrap, equal-version reinstall, blocked downgrade, retained data and reinstall after uninstall'
} catch {
    Get-ChildItem -LiteralPath $root -Filter '*.log' -ErrorAction SilentlyContinue | ForEach-Object { Get-Content -LiteralPath $_.FullName -Tail 40 }
    throw
} finally {
    if (Test-Path -LiteralPath $key) {
        $registered = (Get-ItemProperty -LiteralPath $key).UninstallString.Trim('"')
        if ([IO.Path]::GetDirectoryName($registered) -eq $installed -and (Test-Path -LiteralPath $registered)) {
            Invoke-Program $registered @('/VERYSILENT', '/SUPPRESSMSGBOXES', '/NORESTART') | Out-Null
        }
    }
    if ([IO.Path]::GetDirectoryName([IO.Path]::GetFullPath($root)) -ne [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\')) { throw 'Installer smoke root escaped TEMP' }
    if (Test-Path -LiteralPath $root) { Remove-SmokeDirectory $root }
}
