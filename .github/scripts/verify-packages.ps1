param([Parameter(Mandatory)] [string] $Directory)
$ErrorActionPreference = 'Stop'
$expected = 'win-x64', 'linux-x64', 'osx-arm64', 'osx-x64'
$checksums = @(Get-ChildItem -LiteralPath $Directory -File -Filter '*.sha256')
if ($checksums.Count -ne ($expected.Count + 1)) { throw "Expected five checksums, found $($checksums.Count)" }
$windowsInstaller = $null
foreach ($rid in $expected) {
    $extension = if ($rid -eq 'linux-x64') { 'tar.gz' } else { 'zip' }
    $checksum = @($checksums | Where-Object Name -Like "*-$rid.$extension.sha256")
    if ($checksum.Count -ne 1) { throw "Expected one checksum for $rid" }
    $line = (Get-Content -LiteralPath $checksum[0].FullName -Raw).Trim()
    if ($line -notmatch '^([0-9a-f]{64})  (QueueLoom-[^/\\]+)$') { throw "Invalid checksum: $line" }
    $digest, $name = $Matches[1], $Matches[2]
    if ($name -ne $checksum[0].Name.Replace('.sha256', '')) { throw 'Checksum filename mismatch' }
    $archive = Join-Path $Directory $name
    if ($rid -eq 'win-x64') { $windowsInstaller = $name.Substring(0, $name.Length - 4) + '-setup.exe' }
    if ((Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant() -ne $digest) {
        throw "SHA-256 mismatch: $name"
    }
    if ($extension -eq 'zip') {
        $zip = [IO.Compression.ZipFile]::OpenRead((Resolve-Path -LiteralPath $archive).Path)
        try { $entries = @($zip.Entries.FullName) } finally { $zip.Dispose() }
    } else {
        $entries = @(tar -tzf $archive)
        if ($LASTEXITCODE -ne 0) { throw "Unreadable archive: $name" }
        $entries = @($entries | ForEach-Object { $_ -replace '^\./', '' })
    }
    $required = @('README.md', 'LICENSE', 'THIRD-PARTY-NOTICES.md', 'docs/stable-launcher.md')
    $required += switch -Wildcard ($rid) {
        'win-*' { 'QueueLoom.exe'; 'QueueLoom.bootstrap.json'; 'QueueLoom.bootstrap.zip' }
        'linux-*' { 'QueueLoom'; 'QueueLoom.bootstrap.json'; 'QueueLoom.bootstrap.zip'; 'queueloom.png'; 'install-desktop-entry.sh' }
        'osx-*' { 'QueueLoom.app/Contents/MacOS/QueueLoom'; 'QueueLoom.app/Contents/Info.plist'; 'QueueLoom.app/Contents/Resources/QueueLoom.icns'; 'QueueLoom.app/Contents/Resources/QueueLoom.bootstrap.json'; 'QueueLoom.app/Contents/Resources/initial/manifest.json'; 'QueueLoom.app/Contents/Resources/initial/payload/QueueLoom.app/Contents/MacOS/QueueLoom' }
    }
    foreach ($entry in $required) {
        if ($entry -notin $entries) { throw "Missing $entry in $name" }
    }
    Write-Host "PASS ${name}: checksum and required package contents"
}
$installer = Join-Path $Directory $windowsInstaller
$installerChecksum = (Get-Content -LiteralPath "$installer.sha256" -Raw).Trim()
$installerDigest = (Get-FileHash -LiteralPath $installer -Algorithm SHA256).Hash.ToLowerInvariant()
if ($installerChecksum -cne "$installerDigest  $windowsInstaller") { throw "SHA-256 mismatch: $windowsInstaller" }
$stream = [IO.File]::OpenRead($installer)
try {
    if ($stream.ReadByte() -ne 0x4D -or $stream.ReadByte() -ne 0x5A) { throw "Invalid Windows executable: $windowsInstaller" }
} finally { $stream.Dispose() }
Write-Host "PASS ${windowsInstaller}: expected filename, checksum and executable header"
if (@(Get-ChildItem -LiteralPath $Directory -File).Count -ne 10) { throw 'Expected exactly five package/checksum pairs' }
