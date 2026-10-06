param([Parameter(Mandatory)] [ValidateSet('win-x64', 'linux-x64', 'osx-arm64', 'osx-x64')] [string] $Rid)
$ErrorActionPreference = 'Stop'
$staging = "artifacts/package-$Rid"
$relative = if ($Rid -eq 'win-x64') { 'QueueLoom.exe' } elseif ($Rid.StartsWith('osx-')) {
    'QueueLoom.app/Contents/MacOS/QueueLoom'
} else { 'QueueLoom' }
$data = [IO.Path]::GetFullPath((Join-Path ([IO.Path]::GetTempPath()) ([Guid]::NewGuid().ToString('N'))))
$privatePackage = Join-Path $data 'package'
New-Item -ItemType Directory -Path $privatePackage -Force | Out-Null
Get-ChildItem -LiteralPath $staging -Force | Copy-Item -Destination $privatePackage -Recurse
$executable = (Resolve-Path -LiteralPath (Join-Path $privatePackage $relative)).Path
$start = [Diagnostics.ProcessStartInfo]::new($executable)
$start.ArgumentList.Add('--mcp')
$start.ArgumentList.Add('--read-only')
$start.UseShellExecute = $false
$start.RedirectStandardInput = $true
$start.RedirectStandardOutput = $true
$start.RedirectStandardError = $true
$start.CreateNoWindow = $true
$start.Environment['QUEUELOOM_DATA_DIRECTORY'] = Join-Path $data 'user data'
$start.Environment['QUEUELOOM_BACKUP_DIRECTORY'] = Join-Path $data 'backups'
$start.Environment['DOTNET_BUNDLE_EXTRACT_BASE_DIR'] = Join-Path $data 'bundle-cache'
$process = [Diagnostics.Process]::new()
$process.StartInfo = $start
$started = $false
try {
    if (-not $process.Start()) { throw 'Packaged executable did not start' }
    $started = $true
    $errors = $process.StandardError.ReadToEndAsync()
    $process.StandardInput.WriteLine('{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"package-smoke","version":"1"}}}')
    $line = $process.StandardOutput.ReadLineAsync().WaitAsync([TimeSpan]::FromSeconds(30)).GetAwaiter().GetResult()
    if ($null -eq $line) {
        $process.WaitForExit()
        throw "Packaged server exited ($($process.ExitCode)): $($errors.GetAwaiter().GetResult())"
    }
    $response = $line | ConvertFrom-Json
    if ($response.id -ne 1 -or $response.result.serverInfo.name -ne 'QueueLoom') { throw 'MCP initialization failed' }
    $process.StandardInput.WriteLine('{"jsonrpc":"2.0","method":"notifications/initialized"}')
    $process.StandardInput.WriteLine('{"jsonrpc":"2.0","id":2,"method":"tools/list"}')
    do {
        $line = $process.StandardOutput.ReadLineAsync().WaitAsync([TimeSpan]::FromSeconds(30)).GetAwaiter().GetResult()
        if ($null -eq $line) { throw 'MCP server exited before listing tools' }
        $response = $line | ConvertFrom-Json
    } while ($response.id -ne 2)
    if ('list_environments' -notin $response.result.tools.name -or $response.error) { throw 'MCP tool discovery failed' }
    if (@($response.result.tools | Where-Object { -not $_.annotations.readOnlyHint }).Count) {
        throw 'The read-only packaged server exposed a non-read-only tool'
    }
    Write-Host "PASS $Rid packaged executable: MCP initialization and read-only tool discovery"
} finally {
    if ($started) { $process.StandardInput.Close() }
    # Kill/tree waiting does not join descendants. Join only payloads in this exact private package before
    # removing extracted native libraries which Windows may still have mapped.
    $payloads = @([Diagnostics.Process]::GetProcessesByName('QueueLoom') | Where-Object {
        $_.Id -ne $process.Id -and $_.MainModule.FileName.StartsWith($privatePackage + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)
    })
    if ($started -and -not $process.WaitForExit(15000)) { $process.Kill($true); $process.WaitForExit() }
    foreach ($payload in $payloads) {
        if (-not $payload.HasExited) { $payload.Kill($true); $payload.WaitForExit() }
        $payload.Dispose()
    }
    $process.Dispose()
    # This exact random directory belongs only to this smoke test.
    if (-not $data.StartsWith([IO.Path]::GetFullPath([IO.Path]::GetTempPath()), [StringComparison]::OrdinalIgnoreCase)) { throw 'Smoke directory escaped the temporary workspace' }
    if (Test-Path -LiteralPath $data) { [IO.Directory]::Delete($data, $true) }
}
