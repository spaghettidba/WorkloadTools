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
# Prepare output directory
# ---------------------------------------------------------------------------
$outDir = "$PSScriptRoot\bin\$Platform\Release"
if (-not (Test-Path $outDir)) {
    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
} elseif (Test-Path "$outDir\*" -PathType Any) {
    Remove-Item "$outDir\*" -Force
}

# ---------------------------------------------------------------------------
# Restore WiX SDK NuGet packages (WixToolset.Sdk, Bal, UI extensions)
# ---------------------------------------------------------------------------
& $msbuild "$PSScriptRoot\SetupBootstrapper.wixproj" -t:Restore -nologo -verbosity:minimal

# ---------------------------------------------------------------------------
# Build the bundle EXE
# ---------------------------------------------------------------------------
& $msbuild "$PSScriptRoot\SetupBootstrapper.wixproj" `
    -t:Rebuild `
    -p:Configuration=Release `
    -p:Platform=$Platform `
    "-p:DefineConstants=BuildVersion=$BuildVersion" `
    -nologo -verbosity:minimal

# ---------------------------------------------------------------------------
# Sign (or just rename if no signing cert is configured)
# ---------------------------------------------------------------------------
. $PSScriptRoot\SignMsi.ps1 `
    -InputFile "$outDir\WorkloadTools.exe" `
    -OutputFile "$outDir\WorkloadTools_$Platform.exe"