param (
    [Parameter(Mandatory=$false)]
    [string]$BuildVersion = "1.0.0.0",
    [Parameter(Mandatory=$false)]
    [string]$Platform = "x64"
)

Set-Location $PSScriptRoot

# ---------------------------------------------------------------------------
# Locate MSBuild via vswhere (ships with Visual Studio 2017+)
# ---------------------------------------------------------------------------
$vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
if (-not (Test-Path $vswhere)) {
    throw "vswhere.exe not found at '$vswhere'. Visual Studio 2017 or newer is required."
}

$msbuild = & $vswhere -latest -requires Microsoft.Component.MSBuild `
    -find MSBuild\**\Bin\MSBuild.exe | Select-Object -First 1
if (-not $msbuild) {
    throw "MSBuild.exe not found. Please install Visual Studio with the MSBuild component."
}

# ---------------------------------------------------------------------------
# Locate WiX v3 tools (heat.exe, candle.exe, light.exe)
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

$heat   = Join-Path $wixDir "bin\heat.exe"
$candle = Join-Path $wixDir "bin\candle.exe"
$light  = Join-Path $wixDir "bin\light.exe"

# ---------------------------------------------------------------------------
# Build the .NET projects whose output directories will be harvested into
# the MSI by heat.exe.
#
# SqlWorkload and WorkloadViewer use the "AnyCPU" MSBuild platform to produce
# x64 output (bin\x64\Release) or "x86" for x86 output (bin\x86\Release).
# ConvertWorkload always outputs to bin\Release regardless of platform.
# ---------------------------------------------------------------------------
$netPlatform = if ($Platform -eq 'x86') { 'x86' } else { 'AnyCPU' }

& $msbuild "$PSScriptRoot\..\SqlWorkload\SqlWorkload.csproj" `
    -t:Rebuild -p:Configuration=Release "-p:Platform=$netPlatform" `
    -nologo -verbosity:minimal
if ($LASTEXITCODE -ne 0) { throw "SqlWorkload build failed." }

& $msbuild "$PSScriptRoot\..\WorkloadViewer\WorkloadViewer.csproj" `
    -t:Rebuild -p:Configuration=Release "-p:Platform=$netPlatform" `
    -nologo -verbosity:minimal
if ($LASTEXITCODE -ne 0) { throw "WorkloadViewer build failed." }

& $msbuild "$PSScriptRoot\..\ConvertWorkload\ConvertWorkload.csproj" `
    -t:Rebuild -p:Configuration=Release `
    -nologo -verbosity:minimal
if ($LASTEXITCODE -ne 0) { throw "ConvertWorkload build failed." }

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
# Harvest .NET output directories with heat.exe (WiX v3)
# The generated harvest*.wxs files are placed in the Setup project directory
# so they are picked up as Compile items in Setup.wixproj.
# ---------------------------------------------------------------------------
$sqlWorkloadDir    = [System.IO.Path]::GetFullPath("$PSScriptRoot\..\SqlWorkload\bin\$Platform\Release")
$workloadViewerDir = [System.IO.Path]::GetFullPath("$PSScriptRoot\..\WorkloadViewer\bin\$Platform\Release")
$convertWorkloadDir = [System.IO.Path]::GetFullPath("$PSScriptRoot\..\ConvertWorkload\bin\Release")

& $heat dir "$sqlWorkloadDir" `
    -cg ProductComponents -dr INSTALLFOLDER -srd -sreg -ag `
    -t "$PSScriptRoot\transform.xsl" `
    -out "$PSScriptRoot\harvest.wxs" -nologo
if ($LASTEXITCODE -ne 0) { throw "heat.exe failed for SqlWorkload." }

& $heat dir "$workloadViewerDir" `
    -cg WorkloadViewerComponents -dr INSTALLFOLDER -srd -sreg -ag `
    -t "$PSScriptRoot\transform.xsl" -t "$PSScriptRoot\transform2.xsl" `
    -t "$PSScriptRoot\transform-nodirs.xsl" `
    -out "$PSScriptRoot\harvest2.wxs" -nologo
if ($LASTEXITCODE -ne 0) { throw "heat.exe failed for WorkloadViewer." }

& $heat dir "$convertWorkloadDir" `
    -cg ConvertWorkloadComponents -dr INSTALLFOLDER -srd -sreg -ag `
    -t "$PSScriptRoot\transform.xsl" -t "$PSScriptRoot\transform2.xsl" `
    -t "$PSScriptRoot\transform3.xsl" -t "$PSScriptRoot\transform-nodirs.xsl" `
    -out "$PSScriptRoot\harvest3.wxs" -nologo
if ($LASTEXITCODE -ne 0) { throw "heat.exe failed for ConvertWorkload." }

# ---------------------------------------------------------------------------
# Compile all WXS sources with candle.exe
# ---------------------------------------------------------------------------
$arch = if ($Platform -eq 'x86') { 'x86' } else { 'x64' }

& $candle `
    "$PSScriptRoot\Product.wxs" `
    "$PSScriptRoot\harvest.wxs" `
    "$PSScriptRoot\harvest2.wxs" `
    "$PSScriptRoot\harvest3.wxs" `
    -arch $arch `
    "-dBuildVersion=$BuildVersion" `
    "-dPlatform=$Platform" `
    -out "$objDir\" `
    -nologo
if ($LASTEXITCODE -ne 0) { throw "candle.exe failed." }

# ---------------------------------------------------------------------------
# Link with light.exe to produce the MSI
# ---------------------------------------------------------------------------
$wixObjs = Get-ChildItem "$objDir\*.wixobj" | Select-Object -ExpandProperty FullName

& $light $wixObjs `
    -out "$outDir\WorkloadTools.msi" `
    -nologo
if ($LASTEXITCODE -ne 0) { throw "light.exe failed." }

# ---------------------------------------------------------------------------
# Sign (or just rename if no signing cert is configured)
# ---------------------------------------------------------------------------
. $PSScriptRoot\SignMsi.ps1 `
    -InputFile "$outDir\WorkloadTools.msi" `
    -OutputFile "$outDir\WorkloadTools_$Platform.msi"