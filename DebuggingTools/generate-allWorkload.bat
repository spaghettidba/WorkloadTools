del "c:\temp\workloadtools\debug\capture.sqlite"
sqlcmd -S(local) -dbenchmark -Q"TRUNCATE TABLE dbo.benchmark"
start "" powershell -File .\generate-workload.ps1 -start 0
start "" powershell -File .\generate-workload.ps1 -start 1000000
start "" powershell -File .\generate-workload.ps1 -start 2000000
start "" powershell -File .\generate-workload.ps1 -start 3000000
start "" powershell -File .\generate-workload.ps1 -start 4000000
start "" powershell -File .\generate-workload.ps1 -start 5000000