param (
    [Parameter(Mandatory=$false)]
    [string]$BuildVersion = "1.0.0.0",

    [Parameter(Mandatory=$false)]
    [string]$WixBinPath = "C:\Program Files (x86)\WiX Toolset v3.11\bin"
)

Set-Location $PSScriptRoot

# ----------------------------------------------------------------
$heatArgs1 = @(
    "dir",
    "$PSScriptRoot\..\SqlWorkload\bin\release\net9.0",
    "-gg",
    "-sfrag",
    "-sreg",
    "-srd",
    "-nologo",
    "-cg", "ProductComponents",
    "-dr", "INSTALLFOLDER",
    "-out", "$PSScriptRoot\harvest.wxs",
    "-var", "var.SqlWorkload.TargetDir",
    "-t", "$PSScriptRoot\transform.xsl"
)
& "$WixBinPath\heat.exe" @heatArgs1

# ----------------------------------------------------------------
#$heatArgs2 = @(
#    "dir",
#    "$PSScriptRoot\..\WorkloadViewer\bin\release",
#    "-gg",
#    "-sfrag",
#    "-sreg",
#    "-srd",
#    "-nologo",
#    "-cg", "WorkloadViewerComponents",
#    "-dr", "INSTALLFOLDER",
#    "-out", "$PSScriptRoot\harvest2.wxs",
#    "-var", "var.WorkloadViewer.TargetDir",
#    "-t", "$PSScriptRoot\transform.xsl",
#    "-t", "$PSScriptRoot\transform2.xsl"
#)
#& "$WixBinPath\heat.exe" @heatArgs2

# ----------------------------------------------------------------
$heatArgs3 = @(
    "dir",
    "$PSScriptRoot\..\ConvertWorkload\bin\release\net9.0",
    "-gg",
    "-sfrag",
    "-sreg",
    "-srd",
    "-nologo",
    "-cg", "ConvertWorkloadComponents",
    "-dr", "INSTALLFOLDER",
    "-out", "$PSScriptRoot\harvest3.wxs",
    "-var", "var.ConvertWorkload.TargetDir",
    "-t", "$PSScriptRoot\transform.xsl",
    "-t", "$PSScriptRoot\transform2.xsl",
    "-t", "$PSScriptRoot\transform3.xsl"
)
& "$WixBinPath\heat.exe" @heatArgs3

# ----------------------------------------------------------------
$candleArgs = @(
    "-nologo",
    "-out", "$PSScriptRoot\candleout\",
    "-dSqlWorkload.TargetDir=`"$PSScriptRoot\..\SqlWorkload\bin\release\net9.0`"",
    #"-dWorkloadViewer.TargetDir=`"$PSScriptRoot\..\WorkloadViewer\bin\release`"",
    "-dConvertWorkload.TargetDir=`"$PSScriptRoot\..\ConvertWorkload\bin\release\net9.0`"",
    "-dBuildVersion=$BuildVersion",
    #"-dPlatform=x64",
    "$PSScriptRoot\Product.wxs",
    "$PSScriptRoot\harvest.wxs",
    #"$PSScriptRoot\harvest2.wxs",
    "$PSScriptRoot\harvest3.wxs"
    #"-arch", "x64"
)
& "$WixBinPath\candle.exe" @candleArgs

# ----------------------------------------------------------------
# Light: link the wixobj files to create the MSI
$lightArgs = @(
    "-out", "C:\temp\WorkloadTools-$BuildVersion.msi",
    "$PSScriptRoot\candleout\*.wixobj",
    "-ext", "WixUIExtension"
)
& "$WixBinPath\light.exe" @lightArgs

Write-Host "MSI Generated: C:\temp\WorkloadTools-$BuildVersion.msi"
