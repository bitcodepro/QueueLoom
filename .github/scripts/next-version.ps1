<#
.SYNOPSIS
  Computes the next release version from the existing vX.Y.Z tags.

.DESCRIPTION
  Finds the highest stable tag (pre-release tags such as v1.2.0-rc.1 are ignored) and bumps it.
  Only the first line of the commit message is read: a squash merge puts every commit of the
  pull request into the body, and prose there must not change the release.
  With -Bump auto that line decides the part to bump:
    [major] or #major -> X+1.0.0
    [minor] or #minor -> X.Y+1.0
    anything else     -> X.Y.Z+1
  [skip release] in that line skips the release.
  An explicit ReleaseVersion overrides bump/skip and accepts stable or prerelease SemVer.
  Writes version, tag and skip to $env:GITHUB_OUTPUT when it is set.
#>
param(
    [ValidateSet('auto', 'patch', 'minor', 'major')]
    [string] $Bump = 'auto',
    [string] $CommitMessage = '',
    [string] $ReleaseVersion = ''
)

$ErrorActionPreference = 'Stop'

function Write-Output-Value([string] $name, [string] $value) {
    Write-Host "$name=$value"
    if ($env:GITHUB_OUTPUT) {
        "$name=$value" | Out-File -FilePath $env:GITHUB_OUTPUT -Append -Encoding utf8
    }
}

function Get-LatestStableVersion {
    $tags = @(git tag --list 'v*')
    if ($LASTEXITCODE -ne 0) { throw 'Could not list tags.' }
    $tags | Where-Object { $_ -match '^v([0-9]+)\.([0-9]+)\.([0-9]+)$' } |
        ForEach-Object { [version]($_.Substring(1)) } |
        Sort-Object -Descending | Select-Object -First 1
}

$subject = ($CommitMessage -split "`r?`n", 2)[0]

if ($ReleaseVersion -ne '') {
    # SemVer without build metadata; the value also becomes an archive filename and a tag.
    if ($ReleaseVersion -cnotmatch '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?\z' -or
        @((($ReleaseVersion -split '-', 2)[1] -split '\.') | Where-Object { $_ -match '^0[0-9]+$' }).Count) {
        throw 'release_version must be SemVer without v or build metadata, such as 1.2.3 or 1.2.3-rc.1.'
    }
    if (git tag --list "v$ReleaseVersion") { throw "Tag v$ReleaseVersion already exists." }
    if ($LASTEXITCODE -ne 0) { throw 'Could not list tags.' }
    if (-not $ReleaseVersion.Contains('-')) {
        $latest = Get-LatestStableVersion
        if ($null -ne $latest -and [version]$ReleaseVersion -le $latest) {
            throw "Stable release_version must be greater than v$($latest.ToString(3)). Choose a newer version and start CI on main."
        }
    }
    Write-Output-Value 'skip' 'false'
    Write-Output-Value 'version' $ReleaseVersion
    Write-Output-Value 'tag' "v$ReleaseVersion"
    Write-Output-Value 'prerelease' ($ReleaseVersion.Contains('-').ToString().ToLowerInvariant())
    return
}

if ($subject -match '\[skip release\]') {
    Write-Output-Value 'skip' 'true'
    return
}

if ($Bump -eq 'auto') {
    $Bump = if ($subject -match '\[major\]|#major') { 'major' }
            elseif ($subject -match '\[minor\]|#minor') { 'minor' }
            else { 'patch' }
}

$latest = Get-LatestStableVersion

if ($null -eq $latest) {
    $next = [version]'0.1.0'
}
else {
    $next = switch ($Bump) {
        'major' { [version]::new($latest.Major + 1, 0, 0) }
        'minor' { [version]::new($latest.Major, $latest.Minor + 1, 0) }
        default { [version]::new($latest.Major, $latest.Minor, $latest.Build + 1) }
    }
}

$version = $next.ToString(3)
if (git tag --list "v$version") {
    throw "Tag v$version already exists."
}
if ($LASTEXITCODE -ne 0) { throw 'Could not list tags.' }

Write-Output-Value 'skip' 'false'
Write-Output-Value 'bump' $Bump
Write-Output-Value 'version' $version
Write-Output-Value 'tag' "v$version"
Write-Output-Value 'prerelease' 'false'
