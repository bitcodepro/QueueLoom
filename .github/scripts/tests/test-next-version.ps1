$ErrorActionPreference = 'Stop'
$global:versionTestTags = @('v1.2.3', 'v1.2.4-rc.1')
# No git writes, tags, repository or release APIs: version selection uses a fake tag listing.
function git {
    $global:LASTEXITCODE = 0
    if ($args[0] -ne 'tag' -or $args[1] -ne '--list') { throw 'Unexpected git command' }
    $pattern = $args[2]
    $global:versionTestTags | Where-Object { $_ -like $pattern }
}
$outputFile = [IO.Path]::GetTempFileName()
$previousOutput = $env:GITHUB_OUTPUT
$env:GITHUB_OUTPUT = $outputFile
$versionScript = Join-Path $PSScriptRoot '../next-version.ps1'
$count = 0
function Check-Version([hashtable] $arguments, [hashtable] $expected) {
    [IO.File]::WriteAllText($outputFile, '')
    & $versionScript @arguments
    $actual = @{}
    Get-Content -LiteralPath $outputFile | ForEach-Object {
        $key, $value = $_ -split '=', 2
        $actual[$key] = $value
    }
    foreach ($key in $expected.Keys) {
        if ($actual[$key] -cne $expected[$key]) { throw "Expected $key=$($expected[$key]), got $($actual[$key])" }
    }
    $script:count++
}
function Check-Rejected([string] $version) {
    try { & $versionScript -ReleaseVersion $version } catch { $script:count++; return }
    throw "Accepted invalid/existing version: $version"
}
try {
    Check-Version @{} @{ version = '1.2.4'; tag = 'v1.2.4'; prerelease = 'false'; skip = 'false' }
    Check-Version @{ CommitMessage = '[minor] feature' } @{ version = '1.3.0' }
    Check-Version @{ CommitMessage = '[major] feature' } @{ version = '2.0.0' }
    Check-Version @{ Bump = 'minor' } @{ version = '1.3.0' }
    Check-Version @{ CommitMessage = "ordinary`n[major] body" } @{ version = '1.2.4' }
    Check-Version @{ CommitMessage = '[skip release] docs' } @{ skip = 'true' }
    Check-Version @{ ReleaseVersion = '2.0.0-rc.1'; CommitMessage = '[skip release]' } @{ version = '2.0.0-rc.1'; prerelease = 'true'; skip = 'false' }
    Check-Version @{ ReleaseVersion = '2.0.0' } @{ prerelease = 'false'; version = '2.0.0' }
    Check-Version @{ ReleaseVersion = '0.0.0-alpha-beta.0' } @{ prerelease = 'true' }
    Check-Rejected '1.0.1'
    $global:versionTestTags = @('v1.10.0', 'v1.9.99', 'v9.0.0-rc.1')
    Check-Rejected '1.9.100'
    Check-Rejected '1.10.0'
    Check-Version @{ ReleaseVersion = '1.10.1' } @{ version = '1.10.1'; prerelease = 'false' }
    Check-Version @{} @{ version = '1.10.1' }
    Check-Version @{ ReleaseVersion = '1.9.0-rc.2' } @{ prerelease = 'true' }
    $global:versionTestTags = @('v1.2.3', 'v1.2.4-rc.1')
    foreach ($version in @('1.2.3', 'v2.0.0', '01.2.3', '1.02.3', '1.2.03', '1.2.3-01',
            '1.2.3-rc..1', '1.2.3-', '1.2.3+build', "1.2.3;echo 'bad'", "1.2.3`n", ' 1.2.3', '1.2')) {
        Check-Rejected $version
    }
    $global:versionTestTags = @()
    Check-Version @{} @{ version = '0.1.0' }
    Write-Host "PASS $count version selection/rejection cases"
} finally {
    $env:GITHUB_OUTPUT = $previousOutput
    Remove-Item -LiteralPath $outputFile -Force
    Remove-Variable -Name versionTestTags -Scope Global
}
