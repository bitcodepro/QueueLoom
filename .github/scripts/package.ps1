<#
.SYNOPSIS
  Packages the published app in artifacts/publish for one platform and writes a SHA-256 checksum.
.DESCRIPTION
  win-x64:   QueueLoom-<version>-win-x64.zip       with QueueLoom.exe
  linux-x64: QueueLoom-<version>-linux-x64.tar.gz  with the QueueLoom executable
  osx-*:     QueueLoom-<version>-<rid>.zip          with QueueLoom.app (ad-hoc signed; must run on macOS)
  README, LICENSE and third-party notices are included. Writes archive and checksum to $env:GITHUB_OUTPUT.
#>
param(
    [Parameter(Mandatory)] [ValidateSet('win-x64', 'linux-x64', 'osx-arm64', 'osx-x64')] [string] $Rid,
    [Parameter(Mandatory)] [string] $Version
)

$ErrorActionPreference = 'Stop'
$publish = 'artifacts/publish'
$staging = "artifacts/package-$Rid"
Remove-Item -Recurse -Force $staging -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force $staging | Out-Null
$docs = 'README.md', 'LICENSE', 'THIRD-PARTY-NOTICES.md'
# Native symbol files are not needed at runtime and triple the download size.
$files = Get-ChildItem $publish -File | Where-Object { $_.Extension -notin '.pdb', '.dbg', '.dSYM' }

switch -Wildcard ($Rid) {
    'win-*' {
        $files | Copy-Item -Destination $staging
        Copy-Item $docs -Destination $staging
        $name = "QueueLoom-$Version-$Rid.zip"
        Compress-Archive -Path "$staging/*" -DestinationPath "artifacts/$name" -Force
    }
    'linux-*' {
        $files | Copy-Item -Destination $staging
        Copy-Item $docs -Destination $staging
        chmod +x "$staging/QueueLoom"
        $name = "QueueLoom-$Version-$Rid.tar.gz"
        tar -czf "artifacts/$name" -C $staging .
        if ($LASTEXITCODE -ne 0) { throw 'tar failed' }
    }
    'osx-*' {
        # A minimal app bundle, so QueueLoom opens from Finder like any Mac app.
        $bundle = "$staging/QueueLoom.app"
        $macOS = "$bundle/Contents/MacOS"
        New-Item -ItemType Directory -Force $macOS, "$bundle/Contents/Resources" | Out-Null
        $files | Copy-Item -Destination $macOS
        chmod +x "$macOS/QueueLoom"
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
  <key>CFBundleShortVersionString</key><string>$shortVersion</string>
  <key>CFBundleVersion</key><string>$Version</string>
  <key>LSMinimumSystemVersion</key><string>12.0</string>
  <key>NSHighResolutionCapable</key><true/>
</dict>
</plist>
"@ | Out-File -Encoding utf8 "$bundle/Contents/Info.plist"
        Copy-Item $docs -Destination $staging
        # Apple silicon refuses unsigned code; an ad-hoc signature is enough to start (Gatekeeper still asks once).
        codesign --force --deep --sign - $bundle
        if ($LASTEXITCODE -ne 0) { throw 'codesign failed' }
        $name = "QueueLoom-$Version-$Rid.zip"
        Push-Location $staging
        try {
            # ditto keeps permissions and the signature, which Compress-Archive would drop.
            ditto -c -k --sequesterRsrc --keepParent QueueLoom.app "../$name"
            if ($LASTEXITCODE -ne 0) { throw 'ditto failed' }
            zip -q "../$name" $docs
            if ($LASTEXITCODE -ne 0) { throw 'zip failed' }
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
