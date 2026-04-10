
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
# Build the .NET projects (SqlWorkload, WorkloadViewer, ConvertWorkload, etc.)
# The Setup and SetupBootstrapper WiX projects are excluded from the solution
# build and are handled separately by buildexe.ps1 below.
# ---------------------------------------------------------------------------
& $msbuild "$PSScriptRoot\WorkloadTools.sln" -t:Rebuild -p:Configuration=Release -p:Platform=x64
. $PSScriptRoot\SetupBootstrapper\buildexe.ps1 -Platform x64

& $msbuild "$PSScriptRoot\WorkloadTools.sln" -t:Rebuild -p:Configuration=Release -p:Platform=x86
. $PSScriptRoot\SetupBootstrapper\buildexe.ps1 -Platform x86