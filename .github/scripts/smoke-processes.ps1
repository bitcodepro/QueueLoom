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
