param (
    [Parameter(Mandatory=$false)]
    [string]$BuildVersion = "1.0.0.0",
    [Parameter(Mandatory=$false)]
    [string]$Platform = "x64",
    [Parameter(Mandatory=$false)]
    [string]$WixBinPath = "c:\Program Files (x86)\WiX Toolset v3.11\bin"
)


. $PSScriptRoot\..\Setup\buildmsi.ps1 -BuildVersion $BuildVersion -Platform $Platform -WixBinPath $WixBinPath

Set-Location $PSScriptRoot

# Remove previous builds
If(Test-Path $PSScriptRoot\bin\$Platform\Release\* -PathType Any) {
    Remove-Item $PSScriptRoot\bin\$Platform\Release\*
}


if($BuildVersion -eq "1.0.0.0") {
    # Try to read from SharedAssemblyInfo
    $BuildVersion = (Get-Content ..\SharedAssemblyInfo.cs | Where-Object { $_.StartsWith("[assembly: AssemblyVersion(") }).Replace('[assembly: AssemblyVersion("','').Replace('")]','')
}

#----------------------------------------------------------------


$candleArgs = @(
    "-nologo",
    "-out",
    "$PSScriptRoot\candleout\",
    "-dBuildVersion=$BuildVersion",
    "-dPlatform=$Platform",
    "$PSScriptRoot\Bundle.wxs",
    "-arch",
    "$Platform",
    "-ext",
    "WixBalExtension"
)

# Write-Host $candleArgs

& "$WixBinPath\candle.exe" @candleArgs

#----------------------------------------------------------------

$lightArgs = @(
    "-out",
    ".\bin\Release\WorkloadTools.exe"
    "$PSScriptRoot\candleout\*.wixobj",
    "-ext",
    "WixUIExtension",
    "-ext",
    "WixBalExtension"
)

# Write-Host $lightArgs


& "$WixBinPath\light.exe" @lightArgs

. $PSScriptRoot\SignMsi.ps1 -InputFile ".\bin\Release\WorkloadTools.exe" -OutputFile ".\bin\Release\WorkloadTools_$Platform.exe"