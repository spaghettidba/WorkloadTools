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
# Build the .NET projects whose output directories will be harvested into
# the MSI by the WiX SDK HarvestDirectory items in Setup.wixproj.
#
# SqlWorkload and WorkloadViewer use the "AnyCPU" configuration to produce
# x64 output (bin\x64\Release) and "x86" for x86 output (bin\x86\Release).
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
# Prepare output directory
# ---------------------------------------------------------------------------
$outDir = "$PSScriptRoot\bin\$Platform\Release"
if (-not (Test-Path $outDir)) {
    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
} elseif (Test-Path "$outDir\*" -PathType Any) {
    Remove-Item "$outDir\*" -Force
}

# ---------------------------------------------------------------------------
# Restore WiX SDK NuGet packages (WixToolset.Sdk, Heat, UI extensions)
# ---------------------------------------------------------------------------
& $msbuild "$PSScriptRoot\Setup.wixproj" -t:Restore -nologo -verbosity:minimal

# ---------------------------------------------------------------------------
# Build the MSI
# The WiX SDK handles harvesting (replacing the old heat.exe step) and
# compiling/linking (replacing the old candle.exe + light.exe steps).
# DefineConstants passes the version as a WiX preprocessor variable so that
# $(var.BuildVersion) in Product.wxs resolves to the correct value.
# ---------------------------------------------------------------------------
& $msbuild "$PSScriptRoot\Setup.wixproj" `
    -t:Rebuild `
    -p:Configuration=Release `
    -p:Platform=$Platform `
    "-p:DefineConstants=BuildVersion=$BuildVersion" `
    -nologo -verbosity:minimal
if ($LASTEXITCODE -ne 0) { throw "Setup.wixproj build failed." }

# ---------------------------------------------------------------------------
# Sign (or just rename if no signing cert is configured)
# ---------------------------------------------------------------------------
. $PSScriptRoot\SignMsi.ps1 `
    -InputFile "$outDir\WorkloadTools.msi" `
    -OutputFile "$outDir\WorkloadTools_$Platform.msi"