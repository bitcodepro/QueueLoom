<#
.SYNOPSIS
  Packages the published app in artifacts/publish for one platform and writes a SHA-256 checksum.
.DESCRIPTION
  win-x64:   QueueLoom-<version>-win-x64.zip       with QueueLoom.exe
  linux-x64: QueueLoom-<version>-linux-x64.tar.gz  with the QueueLoom executable, icon and install-desktop-entry.sh
  osx-*:     QueueLoom-<version>-<rid>.zip          with QueueLoom.app and its icon (ad-hoc signed; must run on macOS)
  README, LICENSE and third-party notices are included. Writes archive and checksum to $env:GITHUB_OUTPUT.
#>
param(
    [Parameter(Mandatory)] [ValidateSet('win-x64', 'linux-x64', 'osx-arm64', 'osx-x64')] [string] $Rid,
    [Parameter(Mandatory)] [string] $Version
)

$ErrorActionPreference = 'Stop'
$publish = 'artifacts/publish'
$launcherPublish = 'artifacts/launcher'
$staging = "artifacts/package-$Rid"
$artifactRoot = [IO.Path]::GetFullPath('artifacts')
$staging = [IO.Path]::GetFullPath($staging)
if ([IO.Path]::GetDirectoryName($staging) -ne $artifactRoot) { throw 'Package staging escaped the artifact directory' }
Remove-Item -LiteralPath $staging -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force $staging | Out-Null
$docs = 'README.md', 'LICENSE', 'THIRD-PARTY-NOTICES.md'
$assets = 'src/QueueLoom.App/Assets'
New-Item -ItemType Directory -Path (Join-Path $staging 'docs') | Out-Null
Copy-Item -LiteralPath 'docs/stable-launcher.md' -Destination (Join-Path $staging 'docs/stable-launcher.md')
# Native symbol files are not needed at runtime and triple the download size.
$files = Get-ChildItem $publish -File | Where-Object { $_.Extension -notin '.pdb', '.dbg', '.dSYM' }

function Write-VersionManifest([string] $Initial) {
    $metadata = [ordered]@{}
    $payload = Join-Path $Initial 'payload'
    Get-ChildItem -LiteralPath $payload -File -Recurse | Sort-Object FullName | ForEach-Object {
        $relative = [IO.Path]::GetRelativePath($payload, $_.FullName).Replace('\', '/')
        $mode = if ($IsWindows) { 420 } else { [int][IO.File]::GetUnixFileMode($_.FullName) }
        $metadata[$relative] = @{ Sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant(); UnixMode = $mode }
    }
    $manifest = @{ Protocol = 1; Id = [Guid]::NewGuid().ToString('N'); Rid = $Rid; Version = $Version; Files = $metadata }
    $path = Join-Path $Initial 'manifest.json'
    [IO.File]::WriteAllText($path, ($manifest | ConvertTo-Json -Depth 6))
    return @{ Protocol = 1; Id = $manifest.Id; Rid = $Rid; ManifestSha256 = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant(); ArchiveSha256 = $null }
}

function Write-FlatBootstrap {
    $initial = Join-Path $staging 'bootstrap-content'
    $payload = Join-Path $initial 'payload'
    New-Item -ItemType Directory -Path $payload -Force | Out-Null
    $files | Copy-Item -Destination $payload
    $descriptor = Write-VersionManifest $initial
    $archive = Join-Path $staging 'QueueLoom.bootstrap.zip'
    [IO.Compression.ZipFile]::CreateFromDirectory($initial, $archive)
    $descriptor.ArchiveSha256 = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant()
    [IO.File]::WriteAllText((Join-Path $staging 'QueueLoom.bootstrap.json'), ($descriptor | ConvertTo-Json))
    # This exact staging subtree was created above by this packaging invocation.
    Remove-Item -LiteralPath $initial -Recurse -Force
}

switch -Wildcard ($Rid) {
    'win-*' {
        Write-FlatBootstrap
        Copy-Item -LiteralPath (Join-Path $launcherPublish 'QueueLoom.Launcher.exe') -Destination (Join-Path $staging 'QueueLoom.exe')
        Copy-Item $docs -Destination $staging
        $name = "QueueLoom-$Version-$Rid.zip"
        Compress-Archive -Path "$staging/*" -DestinationPath "artifacts/$name" -Force
    }
    'linux-*' {
        Write-FlatBootstrap
        Copy-Item -LiteralPath (Join-Path $launcherPublish 'QueueLoom.Launcher') -Destination (Join-Path $staging 'QueueLoom')
        Copy-Item $docs -Destination $staging
        Copy-Item "$assets/queueloom-256.png" "$staging/queueloom.png"
        Copy-Item '.github/scripts/install-desktop-entry.sh' -Destination $staging
        chmod +x "$staging/QueueLoom" "$staging/install-desktop-entry.sh"
        $name = "QueueLoom-$Version-$Rid.tar.gz"
        tar -czf "artifacts/$name" -C $staging .
        if ($LASTEXITCODE -ne 0) { throw 'tar failed' }
    }
    'osx-*' {
        # A minimal app bundle, so QueueLoom opens from Finder like any Mac app.
        $bundle = "$staging/QueueLoom.app"
        $macOS = "$bundle/Contents/MacOS"
        New-Item -ItemType Directory -Force $macOS, "$bundle/Contents/Resources" | Out-Null
        Copy-Item -LiteralPath (Join-Path $launcherPublish 'QueueLoom.Launcher') -Destination (Join-Path $macOS 'QueueLoom')
        chmod +x "$macOS/QueueLoom"
        Copy-Item "$assets/queueloom.icns" "$bundle/Contents/Resources/QueueLoom.icns"
        $shortVersion = ($Version -split '-')[0]
        @"
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key><string>QueueLoom</string>
  <key>CFBundleDisplayName</key><string>QueueLoom</string>
  <key>CFBundleIdentifier</key><string>io.github.bitcodepro.queueloom</string>
  <key>CFBundleExecutable</key><string>QueueLoom</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleIconFile</key><string>QueueLoom</string>
  <key>CFBundleShortVersionString</key><string>$shortVersion</string>
  <key>CFBundleVersion</key><string>$Version</string>
  <key>LSMinimumSystemVersion</key><string>12.0</string>
  <key>NSHighResolutionCapable</key><true/>
</dict>
</plist>
"@ | Out-File -Encoding utf8 "$bundle/Contents/Info.plist"
        Copy-Item $docs -Destination $staging
        # The outer app remains unchanged during updates. Its immutable initial payload makes a single .app
        # portable; new versions and activation state live beside it, outside its sealed resources.
        $initial = Join-Path $bundle 'Contents/Resources/initial'
        $payloadBundle = Join-Path $initial 'payload/QueueLoom.app'
        New-Item -ItemType Directory -Force (Join-Path $payloadBundle 'Contents/MacOS'), (Join-Path $payloadBundle 'Contents/Resources') | Out-Null
        $files | Copy-Item -Destination (Join-Path $payloadBundle 'Contents/MacOS')
        chmod +x (Join-Path $payloadBundle 'Contents/MacOS/QueueLoom')
        Copy-Item -LiteralPath (Join-Path $bundle 'Contents/Info.plist') -Destination (Join-Path $payloadBundle 'Contents/Info.plist')
        Copy-Item -LiteralPath (Join-Path $bundle 'Contents/Resources/QueueLoom.icns') -Destination (Join-Path $payloadBundle 'Contents/Resources/QueueLoom.icns')
        codesign --force --deep --sign - $payloadBundle
        if ($LASTEXITCODE -ne 0) { throw 'Payload codesign failed' }
        $descriptor = Write-VersionManifest $initial
        [IO.File]::WriteAllText((Join-Path $bundle 'Contents/Resources/QueueLoom.bootstrap.json'), ($descriptor | ConvertTo-Json))
        # Apple silicon refuses unsigned code; an ad-hoc signature is enough to start (Gatekeeper still asks once).
        # Do not re-sign the nested payload after its digest has been pinned in the manifest.
        codesign --force --sign - $bundle
        if ($LASTEXITCODE -ne 0) { throw 'codesign failed' }
        $name = "QueueLoom-$Version-$Rid.zip"
        Push-Location $staging
        try {
            # ditto keeps permissions and the signature, which Compress-Archive would drop.
            ditto -c -k --sequesterRsrc --keepParent QueueLoom.app "../$name"
            if ($LASTEXITCODE -ne 0) { throw 'ditto failed' }
            zip -q "../$name" $docs
            if ($LASTEXITCODE -ne 0) { throw 'zip failed' }
            zip -q -r "../$name" docs
            if ($LASTEXITCODE -ne 0) { throw 'Documentation zip failed' }
        }
        finally {
            Pop-Location
        }
    }
}

$hash = (Get-FileHash "artifacts/$name" -Algorithm SHA256).Hash.ToLowerInvariant()
# Same "<hash>  <file>" format as sha256sum, so `sha256sum -c` works.
"$hash  $name" | Out-File -Encoding ascii -NoNewline "artifacts/$name.sha256"
Write-Host "Packaged artifacts/$name"
if ($env:GITHUB_OUTPUT) {
    "archive=artifacts/$name" | Out-File -FilePath $env:GITHUB_OUTPUT -Append -Encoding utf8
    "checksum=artifacts/$name.sha256" | Out-File -FilePath $env:GITHUB_OUTPUT -Append -Encoding utf8
}
