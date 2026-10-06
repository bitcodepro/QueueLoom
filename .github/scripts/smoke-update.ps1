param(
    [string] $Executable = 'artifacts/publish/QueueLoom.exe',
    [string] $FixtureDirectory = 'tests/QueueLoom.UpdateFixture/bin/Release/net10.0',
    [string] $StablePackageDirectory
)
$ErrorActionPreference = 'Stop'
if (-not $IsWindows) { throw 'This packaged GUI smoke test requires Windows.' }
$package = (Resolve-Path -LiteralPath $Executable).Path
$fixture = (Resolve-Path -LiteralPath $FixtureDirectory).Path
$root = [IO.Path]::GetFullPath((Join-Path ([IO.Path]::GetTempPath()) ('QueueLoom update smoke with spaces ' + [Guid]::NewGuid().ToString('N'))))
$id = [Guid]::NewGuid().ToString('N')
$exe = Join-Path $root 'QueueLoom.exe'
$backup = "$exe.$id.old"
$receiptPath = Join-Path $root '.queueloom-update.json'
$parent = $null
$helper = $null
function Start-Isolated([string] $Path, [string[]] $Arguments) {
    $start = [Diagnostics.ProcessStartInfo]::new($Path)
    $start.UseShellExecute = $false
    $start.WorkingDirectory = $root
    $start.WindowStyle = 'Hidden'
    $start.Environment['QUEUELOOM_DATA_DIRECTORY'] = Join-Path $root 'isolated data'
    $start.Environment['QUEUELOOM_BACKUP_DIRECTORY'] = Join-Path $root 'isolated backups'
    $start.Environment['DOTNET_BUNDLE_EXTRACT_BASE_DIR'] = Join-Path $root 'bundle cache'
    foreach ($argument in $Arguments) { $start.ArgumentList.Add($argument) }
    return [Diagnostics.Process]::Start($start)
}
try {
    New-Item -ItemType Directory -Path $root | Out-Null
    Get-ChildItem -LiteralPath $fixture -File | Copy-Item -Destination $root
    Copy-Item -LiteralPath (Join-Path $fixture 'QueueLoom.UpdateFixture.exe') -Destination $exe
    $lock = Join-Path $root 'storage.lock'
    $parent = Start-Isolated $exe @('--hold-lock', $lock, '2000')
    $watch = [Diagnostics.Stopwatch]::StartNew()
    while (-not (Test-Path -LiteralPath "$lock.held")) {
        if ($watch.Elapsed.TotalSeconds -gt 15) { throw 'The isolated old process did not start.' }
        Start-Sleep -Milliseconds 50
    }
    Move-Item -LiteralPath $exe -Destination $backup
    if ($StablePackageDirectory) {
        # Reproduce the legacy top-level-file installer, including its bootstrap descriptor/archive.
        Get-ChildItem -LiteralPath $StablePackageDirectory -File | Copy-Item -Destination $root
    } else { Copy-Item -LiteralPath $package -Destination $exe }
    $download = Join-Path $root "downloads/1.5.4-$id"
    New-Item -ItemType Directory -Path $download | Out-Null
    [IO.File]::WriteAllText((Join-Path $download '.queueloom-download'), $id)
    [IO.File]::WriteAllText((Join-Path $download 'downloaded-package.zip'), 'owned test download')
    [IO.File]::WriteAllText((Join-Path $root 'user-notes.old'), 'keep')
    $receipt = @{
        Id = $id
        Target = @{ Rid = 'win-x64'; InstallDirectory = $root; Executable = $exe; Bundle = $null }
        DownloadDirectory = $download
        Entries = @(@{ Current = $exe; Backup = $backup })
    }
    [IO.File]::WriteAllText($receiptPath, ($receipt | ConvertTo-Json -Depth 8))
    $helper = if ($StablePackageDirectory) {
        Start-Isolated (Join-Path $fixture 'QueueLoom.UpdateFixture.exe') @('--legacy-handoff', $exe, (Join-Path $root 'handoff.lock'), '2000')
    } else { Start-Isolated $exe @('--update-helper', $receiptPath, $id, $parent.Id.ToString(), $parent.StartTime.ToUniversalTime().Ticks.ToString()) }
    Start-Sleep -Milliseconds 250
    if (-not (Test-Path -LiteralPath $backup)) { throw 'The backup was deleted while the previous process was running.' }
    if (-not $helper.WaitForExit(45000)) { throw 'The packaged restart helper timed out.' }
    if ($helper.ExitCode -ne 0) {
        $errorFile = "$receiptPath.error"
        $detail = if (Test-Path -LiteralPath $errorFile) { [IO.File]::ReadAllText($errorFile) } else { 'No diagnostic file' }
        throw "Packaged restart helper failed ($($helper.ExitCode)): $detail"
    }
    $cleanupWait = [Diagnostics.Stopwatch]::StartNew()
    while (Test-Path -LiteralPath $receiptPath) {
        if ($cleanupWait.Elapsed.TotalSeconds -gt 45) { throw 'Legacy handoff did not finish startup/cleanup.' }
        Start-Sleep -Milliseconds 50
    }
    if ((Test-Path -LiteralPath $backup) -or (Test-Path -LiteralPath $download)) {
        throw 'Owned update files were not cleaned after confirmed GUI startup.'
    }
    if ([IO.File]::ReadAllText((Join-Path $root 'user-notes.old')) -ne 'keep') { throw 'An unrelated file was changed.' }
    $updated = @(Get-Process QueueLoom -ErrorAction SilentlyContinue | Where-Object {
        if ($StablePackageDirectory) { $_.Path -like "$root\QueueLoom.versions\versions\*\payload\QueueLoom.exe" } else { $_.Path -eq $exe }
    })
    if ($updated.Count -ne 1) { throw "Expected one running updated GUI payload; found $($updated.Count)." }
    Write-Host 'PASS packaged Windows update: delayed exit, paths with spaces, confirmed GUI startup, owned-file cleanup and user-file preservation.'
} finally {
    foreach ($process in @($parent, $helper)) {
        if ($null -ne $process) {
            if (-not $process.HasExited) { $process.Kill(); $process.WaitForExit() }
            $process.Dispose()
        }
    }
    # Match the exact random installation before touching any GUI process.
    Get-Process QueueLoom -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq $exe -or $_.Path -like "$root\QueueLoom.versions\versions\*\payload\QueueLoom.exe" } | ForEach-Object {
        if (-not $_.HasExited) { $_.Kill($true); $_.WaitForExit() }; $_.Dispose()
    }
    $resolved = [IO.Path]::GetFullPath($root)
    $temp = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    if (-not $resolved.StartsWith($temp, [StringComparison]::OrdinalIgnoreCase) -or
        -not ([IO.Path]::GetFileName($resolved)).StartsWith('QueueLoom update smoke with spaces ')) { throw 'Unexpected smoke-test cleanup path.' }
    $cleanup = [Diagnostics.Stopwatch]::StartNew()
    while (Test-Path -LiteralPath $resolved) {
        try { [IO.Directory]::Delete($resolved, $true) }
        catch {
            if ($cleanup.Elapsed.TotalSeconds -gt 10) { throw }
            Start-Sleep -Milliseconds 100
        }
    }
}
