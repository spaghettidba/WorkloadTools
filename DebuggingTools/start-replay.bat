echo %time%
sqlcmd -S(local) -dbenchmark -Q"TRUNCATE TABLE benchmark"
"c:\program files\workloadtools\sqlworkload.exe" --File "%CD%\replay.json"
echo %time%