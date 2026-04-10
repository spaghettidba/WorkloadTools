param (
    [Parameter(Mandatory=$false)]
    [string]$BuildVersion = "1.0.0.0",
    [Parameter(Mandatory=$false)]
    [string]$Platform = "x64"
)

# ---------------------------------------------------------------------------
# Build the MSI first
# ---------------------------------------------------------------------------
. $PSScriptRoot\..\Setup\buildmsi.ps1 -BuildVersion $BuildVersion -Platform $Platform

Set-Location $PSScriptRoot

# ---------------------------------------------------------------------------
# Resolve the version from SharedAssemblyInfo.cs if the caller left the default
# ---------------------------------------------------------------------------
if ($BuildVersion -eq "1.0.0.0") {
    $BuildVersion = (Get-Content ..\SharedAssemblyInfo.cs |
        Where-Object { $_.StartsWith("[assembly: AssemblyVersion(") }).
        Replace('[assembly: AssemblyVersion("','').Replace('")]','')
}

# ---------------------------------------------------------------------------
# Locate WiX v3 tools (candle.exe, light.exe)
# The WIX environment variable is set automatically by the WiX v3 installer.
# ---------------------------------------------------------------------------
$wixDir = $null
if ($env:WIX -and (Test-Path $env:WIX)) {
    $wixDir = $env:WIX
}
if (-not $wixDir) {
    $wixDir = @(
        "${env:ProgramFiles(x86)}\WiX Toolset v3.14",
        "${env:ProgramFiles(x86)}\WiX Toolset v3.11",
        "${env:ProgramFiles(x86)}\WiX Toolset v3.10",
        "${env:ProgramFiles}\WiX Toolset v3.14",
        "${env:ProgramFiles}\WiX Toolset v3.11"
    ) | Where-Object { Test-Path $_ } | Select-Object -First 1
}
if (-not $wixDir) {
    throw "WiX Toolset v3 not found. Install from https://wixtoolset.org/ or set the WIX environment variable."
}

$candle = Join-Path $wixDir "bin\candle.exe"
$light  = Join-Path $wixDir "bin\light.exe"

# ---------------------------------------------------------------------------
# Prepare output and intermediate directories
# ---------------------------------------------------------------------------
$outDir = "$PSScriptRoot\bin\$Platform\Release"
$objDir = "$PSScriptRoot\obj\$Platform\Release"

foreach ($dir in $outDir, $objDir) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    } elseif (Test-Path "$dir\*") {
        Remove-Item "$dir\*" -Recurse -Force
    }
}

# ---------------------------------------------------------------------------
# Compile Bundle.wxs with candle.exe
# ---------------------------------------------------------------------------
$arch = if ($Platform -eq 'x86') { 'x86' } else { 'x64' }

& $candle `
    "$PSScriptRoot\Bundle.wxs" `
    -arch $arch `
    "-dBuildVersion=$BuildVersion" `
    "-dPlatform=$Platform" `
    -out "$objDir\" `
    -nologo -ext WixBalExtension
if ($LASTEXITCODE -ne 0) { throw "candle.exe failed for Bundle.wxs." }

# ---------------------------------------------------------------------------
# Link with light.exe to produce the bootstrapper EXE
# ---------------------------------------------------------------------------
$wixObjs = Get-ChildItem "$objDir\*.wixobj" | Select-Object -ExpandProperty FullName

& $light $wixObjs `
    -out "$outDir\WorkloadTools.exe" `
    -nologo -ext WixBalExtension
if ($LASTEXITCODE -ne 0) { throw "light.exe failed for Bundle." }

# ---------------------------------------------------------------------------
# Sign (or just rename if no signing cert is configured)
# ---------------------------------------------------------------------------
. $PSScriptRoot\SignMsi.ps1 `
    -InputFile "$outDir\WorkloadTools.exe" `
    -OutputFile "$outDir\WorkloadTools_$Platform.exe"