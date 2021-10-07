[CmdletBinding()]
Param(
    [Parameter(Mandatory=$True,Position=1)]
    [string]$InputFile,
    [Parameter(Mandatory=$True,Position=2)]
    [string]$OutputFile
)


if(-not (Test-Path $PSScriptRoot\SignParams.ps1)) 
{
    Write-Warning "No code signing is applied to the .msi file."
    Write-Warning "You need to create a file called SignParams.ps1 and provide signing info."

    Write-Output "Moving $InputFile --> $OutputFile"
    Move-Item $InputFile $OutputFile -Force

    exit
}

# read paramters
$signParams = get-content $PSScriptRoot\SignParams.ps1 -Raw
Invoke-Expression $signParams

$params = $(
     'sign'
    ,'/f'
    ,('"' + $certPath + '"')
    ,'/p'
    ,('"' + $certPass + '"')
    ,'/sha1'
    ,$certSha
    ,'/t'
    ,('"' + $certTime + '"')
    ,'/d'
    ,'"WorkloadTools"'
)

& $signTool ($params + $InputFile)

Write-Output "Moving $InputFile --> $OutputFile"
Move-Item $InputFile $OutputFile -Force
