param (
    [Parameter(Mandatory=$false)]
    [string]$BuildVersion = "1.0.0.0",
    [Parameter(Mandatory=$false)]
    [string]$Platform = "x64",
    [Parameter(Mandatory=$false)]
    [string]$WixBinPath = "c:\Program Files (x86)\WiX Toolset v3.11\bin"
)

Set-Location $PSScriptRoot

# Remove previous builds
If(Test-Path $PSScriptRoot\bin\$Platform\Release\* -PathType Any) {
    Remove-Item $PSScriptRoot\bin\$Platform\Release\*
}



$heatArgs = @(
    "dir",
    "$PSScriptRoot\..\SqlWorkload\bin\$Platform\release",
    "-gg",
    "-sfrag",
    "-sreg",
    "-srd",
    "-nologo",
    "-cg",
    "ProductComponents",
    "-dr"
    "INSTALLFOLDER",
    "-out",
    "$PSScriptRoot\harvest.wxs",
    "-var",
    "var.SqlWorkload.TargetDir",
    "-t",
    "$PSScriptRoot\transform.xsl"
)

# Write-Host $heatArgs

& "$WixBinPath\heat.exe" @heatArgs




$heatArgs = @(
    "dir",
    "$PSScriptRoot\..\WorkloadViewer\bin\$Platform\release",
    "-gg",
    "-sfrag",
    "-sreg",
    "-srd",
    "-nologo",
    "-cg",
    "WorkloadViewerComponents",
    "-dr"
    "INSTALLFOLDER",
    "-out",
    "$PSScriptRoot\harvest2.wxs",
    "-var",
    "var.WorkloadViewer.TargetDir",
    "-t",
    "$PSScriptRoot\transform.xsl",
    "-t",
    "$PSScriptRoot\transform2.xsl"
)


#  Write-Host "$WixBinPath\heat.exe"
#  Write-Host $heatArgs

& "$WixBinPath\heat.exe" @heatArgs







$heatArgs = @(
    "dir",
    "$PSScriptRoot\..\ConvertWorkload\bin\release",
    "-gg",
    "-sfrag",
    "-sreg",
    "-srd",
    "-nologo",
    "-cg",
    "ConvertWorkloadComponents",
    "-dr"
    "INSTALLFOLDER",
    "-out",
    "$PSScriptRoot\harvest3.wxs",
    "-var",
    "var.ConvertWorkload.TargetDir",
    "-t",
    "$PSScriptRoot\transform.xsl",
    "-t",
    "$PSScriptRoot\transform2.xsl"
    "-t",
    "$PSScriptRoot\transform3.xsl"
)

#  Write-Host "$WixBinPath\heat.exe"
#  Write-Host $heatArgs

& "$WixBinPath\heat.exe" @heatArgs


#----------------------------------------------------------------


$candleArgs = @(
    "-nologo",
    "-out",
    "$PSScriptRoot\candleout\",
    "-dSqlWorkload.TargetDir=`"$PSScriptRoot\..\SqlWorkload\bin\$Platform\release`""
    "-dWorkloadViewer.TargetDir=`"$PSScriptRoot\..\WorkloadViewer\bin\$Platform\release`""
    "-dConvertWorkload.TargetDir=`"$PSScriptRoot\..\ConvertWorkload\bin\release`""
    "-dBuildVersion=$BuildVersion",
    "-dPlatform=$Platform",
    "$PSScriptRoot\Product.wxs",
    "$PSScriptRoot\harvest.wxs",
    "$PSScriptRoot\harvest2.wxs",
    "$PSScriptRoot\harvest3.wxs",
    "-arch"
    "$Platform"
)

# Write-Host $candleArgs

& "$WixBinPath\candle.exe" @candleArgs

#----------------------------------------------------------------

$lightArgs = @(
    "-out",
    ".\bin\$Platform\Release\WorkloadTools.msi"
    "$PSScriptRoot\candleout\*.wixobj",
    "-ext",
    "WixUIExtension"
)

# Write-Host $lightArgs


& "$WixBinPath\light.exe" @lightArgs


. $PSScriptRoot\SignMsi.ps1 -InputFile ".\bin\$Platform\Release\WorkloadTools.msi" -OutputFile ".\bin\$Platform\Release\WorkloadTools_$Platform.msi"