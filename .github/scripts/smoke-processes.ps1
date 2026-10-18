# Process enumeration is a snapshot. A payload can exit between enumeration and MainModule access.
function Test-SmokeProcessOwnership($Process, [string] $Root, [int] $LauncherId) {
    try {
        if ($Process.Id -eq $LauncherId) { return $false }
        $filename = $Process.MainModule.FileName
        if ([string]::IsNullOrEmpty($filename)) { return $false }
        $comparison = if ($IsWindows) { [StringComparison]::OrdinalIgnoreCase } else { [StringComparison]::Ordinal }
        return $filename.StartsWith($Root + [IO.Path]::DirectorySeparatorChar, $comparison)
    } catch [ComponentModel.Win32Exception], [InvalidOperationException] {
        # Exited or inaccessible processes cannot be identified as owned. Never guess by process name.
        return $false
    }
}

function Get-OwnedSmokeProcesses([string] $Root, [int] $LauncherId) {
    foreach ($candidate in [Diagnostics.Process]::GetProcessesByName('QueueLoom')) {
        if (Test-SmokeProcessOwnership $candidate $Root $LauncherId) { $candidate }
        else { $candidate.Dispose() }
    }
}

# Every process whose executable lies under $Root, whatever its name (a payload may be renamed or started late).
function Get-ProcessesUnder([string] $Root) {
    foreach ($candidate in [Diagnostics.Process]::GetProcesses()) {
        if (Test-SmokeProcessOwnership $candidate $Root -1) { $candidate }
        else { $candidate.Dispose() }
    }
}

# Deletes a smoke test's private directory within a bound. Each failed attempt names the file that could not be removed
# and stops any process still running from inside the directory, then tries again; when the bound runs out the error
# says which processes were found, so a lock is diagnosed instead of retried blindly or ignored.
function Remove-SmokeDirectory([string] $Root, [TimeSpan] $Bound = [TimeSpan]::FromSeconds(60)) {
    $deadline = [DateTime]::UtcNow + $Bound
    $attempt = 0
    while ($true) {
        if (-not (Test-Path -LiteralPath $Root)) { return }
        $attempt++
        try { [IO.Directory]::Delete($Root, $true); return }
        catch [IO.IOException], [UnauthorizedAccessException] {
            $failure = $_.Exception.Message
            $holders = @(Get-ProcessesUnder $Root)
            $names = @($holders | ForEach-Object { "$($_.Id) $($_.MainModule.FileName)" })
            Write-Host "Cleanup attempt $attempt of '$Root' failed: $failure Processes running from it: $(if ($names.Count) { $names -join '; ' } else { 'none' })"
            foreach ($holder in $holders) {
                try { if (-not $holder.HasExited) { $holder.Kill($true); [void] $holder.WaitForExit(15000) } }
                finally { $holder.Dispose() }
            }
            if ([DateTime]::UtcNow -ge $deadline) {
                throw "Could not remove '$Root' within $($Bound.TotalSeconds) s after $attempt attempts: $failure Last processes running from it: $(if ($names.Count) { $names -join '; ' } else { 'none (a handle held outside the package, such as a scanner)' })"
            }
            Start-Sleep -Milliseconds 500
        }
    }
}
