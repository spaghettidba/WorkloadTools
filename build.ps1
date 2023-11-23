
& msbuild.exe -t:Rebuild -p:Configuration=Release -p:Platform=x64
. $PSScriptRoot\SetupBootstrapper\buildexe.ps1 -Platform x64
& msbuild.exe -t:Rebuild -p:Configuration=Release -p:Platform=x86
. $PSScriptRoot\SetupBootstrapper\buildexe.ps1 -Platform x86