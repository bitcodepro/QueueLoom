param(
    [Parameter(Mandatory)] [string] $Version,
    [Parameter(Mandatory)] [string] $InputDirectory,
    [Parameter(Mandatory)] [string] $OutputDirectory
)
$ErrorActionPreference = 'Stop'
$compiler = Join-Path ${env:ProgramFiles(x86)} 'Inno Setup 6/ISCC.exe'
if (-not (Test-Path -LiteralPath $compiler -PathType Leaf)) { throw "Inno Setup compiler not found: $compiler" }
if ($Version -cnotmatch '^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$') { throw 'Invalid installer version' }
$inputPath = (Resolve-Path -LiteralPath $InputDirectory).Path
$outputPath = (Resolve-Path -LiteralPath $OutputDirectory).Path
# ISCC defines are passed as individual literal arguments, including paths containing spaces.
& $compiler "/DProductVersion=$Version" "/DPackageInput=$inputPath" "/DPackageOutput=$outputPath" (Join-Path $PSScriptRoot '../installer/QueueLoom.iss')
if ($LASTEXITCODE -ne 0) { throw 'Inno Setup compilation failed' }
