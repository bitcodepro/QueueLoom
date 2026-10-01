param([Parameter(Mandatory)] [ValidateSet('win-x64', 'linux-x64', 'osx-arm64', 'osx-x64')] [string] $Rid)
$ErrorActionPreference = 'Stop'
$staging = "artifacts/package-$Rid"
$relative = if ($Rid -eq 'win-x64') { 'QueueLoom.exe' } elseif ($Rid.StartsWith('osx-')) {
    'QueueLoom.app/Contents/MacOS/QueueLoom'
} else { 'QueueLoom' }
$executable = (Resolve-Path -LiteralPath (Join-Path $staging $relative)).Path
$data = Join-Path ([IO.Path]::GetTempPath()) ([Guid]::NewGuid().ToString('N'))
$start = [Diagnostics.ProcessStartInfo]::new($executable)
$start.ArgumentList.Add('--mcp')
$start.ArgumentList.Add('--read-only')
$start.UseShellExecute = $false
$start.RedirectStandardInput = $true
$start.RedirectStandardOutput = $true
$start.RedirectStandardError = $true
$start.CreateNoWindow = $true
$start.Environment['QUEUELOOM_DATA_DIRECTORY'] = $data
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
    if ($started -and -not $process.HasExited) { $process.Kill($true); $process.WaitForExit() }
    $process.Dispose()
    # This exact random directory belongs only to this smoke test.
    if (Test-Path -LiteralPath $data) { Remove-Item -LiteralPath $data -Recurse -Force }
}
