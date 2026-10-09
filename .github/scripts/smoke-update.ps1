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
$sender = $null
$helperLauncher = $null
$helper = $null
function Get-SmokeProcesses {
    try {
        # Diagnostics are best effort and restricted to this exact installation and sender PID.
        Get-CimInstance Win32_Process -Filter "Name = 'QueueLoom.exe' OR Name = 'QueueLoom.UpdateFixture.exe'" | Where-Object {
            ($_.ExecutablePath -and $_.ExecutablePath.StartsWith($root + '\', [StringComparison]::OrdinalIgnoreCase)) -or
            ($null -ne $sender -and $_.ProcessId -eq $sender.Id)
        } | Select-Object ProcessId, ParentProcessId, ExecutablePath, CommandLine, CreationDate
    } catch {
        # Never replace a helper failure or GUI assertion with a diagnostics-only CIM error.
        [pscustomobject]@{ Diagnostics = 'Unavailable'; ErrorType = $_.Exception.GetType().Name }
    }
}
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
    if ($StablePackageDirectory) {
        $handoffLock = Join-Path $root 'handoff.lock'
        $claimed = Join-Path $root 'handoff-helper-claimed'
        $sender = Start-Isolated (Join-Path $fixture 'QueueLoom.UpdateFixture.exe') @('--legacy-handoff', $exe, $handoffLock, '2000', $claimed)
        $identityPath = "$handoffLock.helper.json"
        $identityWait = [Diagnostics.Stopwatch]::StartNew()
        while (-not (Test-Path -LiteralPath $identityPath)) {
            if ($sender.HasExited) { throw "Legacy handoff sender exited before publishing helper identity ($($sender.ExitCode))." }
            if ($identityWait.Elapsed.TotalSeconds -gt 15) { throw 'Legacy handoff did not publish helper identity.' }
            Start-Sleep -Milliseconds 50
        }
        $identity = [IO.File]::ReadAllText($identityPath) | ConvertFrom-Json
        $candidate = [Diagnostics.Process]::GetProcessById($identity.Pid)
        try {
            $null = $candidate.Handle
            if ($candidate.StartTime.ToUniversalTime().Ticks -ne $identity.StartTicks -or $candidate.Path -ne $exe) {
                throw 'Legacy handoff published an unexpected restart helper identity.'
            }
            $helperLauncher = $candidate # Register for cleanup only after proving ownership.
            $candidate = $null
        } finally {
            if ($null -ne $candidate) { $candidate.Dispose() } # Rejected identities must never be killed.
        }
        # Start returns the stable entry's PID. Its direct payload child runs --update-helper.
        # The sender remains alive until claimed, so this helper cannot finish or be replaced during capture.
        $payloads = @(Get-CimInstance Win32_Process -Filter "ParentProcessId = $($helperLauncher.Id) AND Name = 'QueueLoom.exe'" | Where-Object {
            $_.ExecutablePath -like "$root\QueueLoom.versions\versions\*\payload\QueueLoom.exe" -and
            $_.CommandLine.Contains('--update-helper') -and $_.CommandLine.Contains($receiptPath) -and $_.CommandLine.Contains($id)
        })
        if ($payloads.Count -ne 1) { throw "Expected one restart helper payload; found $($payloads.Count)." }
        $candidate = [Diagnostics.Process]::GetProcessById($payloads[0].ProcessId)
        try {
            $null = $candidate.Handle
            # CIM formats the OS birth time at microsecond precision; allow 5 ms for provider conversion.
            # This tolerance is never ownership proof alone: PID, live verified parent, installation path,
            # helper command/receipt/ID and the retained process handle must all agree while the sender waits.
            if ($payloads[0].ParentProcessId -ne $helperLauncher.Id -or $candidate.Path -ne $payloads[0].ExecutablePath -or
                ($candidate.StartTime.ToUniversalTime() - $payloads[0].CreationDate.ToUniversalTime()).Duration().TotalMilliseconds -gt 5) {
                throw 'The restart helper payload identity changed during capture.'
            }
            $helper = $candidate
            $candidate = $null
        } finally {
            if ($null -ne $candidate) { $candidate.Dispose() }
        }
        [IO.File]::WriteAllText($claimed, 'claimed')
    } else {
        $helper = Start-Isolated $exe @('--update-helper', $receiptPath, $id, $parent.Id.ToString(), $parent.StartTime.ToUniversalTime().Ticks.ToString())
    }
    Start-Sleep -Milliseconds 250
    if (-not (Test-Path -LiteralPath $backup)) { throw 'The backup was deleted while the previous process was running.' }
    Write-Host ('Update process identities: ' + (@(Get-SmokeProcesses) | ConvertTo-Json -Depth 4 -Compress))
    if ($null -ne $sender) {
        if (-not $sender.WaitForExit(45000)) { throw 'The legacy handoff sender timed out.' }
        if ($sender.ExitCode -ne 0) { throw "Legacy handoff sender failed ($($sender.ExitCode))." }
    }
    # The sender's exit and receipt deletion both precede the real helper's exit in stable-package mode.
    Write-Host "Waiting for restart helper PID $($helper.Id), start ticks $($helper.StartTime.ToUniversalTime().Ticks)."
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
    if ($updated.Count -ne 1) {
        $identities = @(Get-SmokeProcesses) | ConvertTo-Json -Depth 4 -Compress
        throw "Expected one running updated GUI payload; found $($updated.Count). Process identities: $identities"
    }
    Write-Host 'PASS packaged Windows update: delayed exit, paths with spaces, confirmed GUI startup, owned-file cleanup and user-file preservation.'
} finally {
    foreach ($process in @($parent, $sender, $helper, $helperLauncher)) {
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
