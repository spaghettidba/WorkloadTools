del "c:\temp\workloadtools\debug\capture.sqlite"
sqlcmd -S(local)\sqlexpress2016 -dlsctbenchmark -Q"TRUNCATE TABLE test.Applications"
sqlcmd -S(local)\sqlexpress2016 -dlsctbenchmark -Q"TRUNCATE TABLE test.Databases"
sqlcmd -S(local)\sqlexpress2016 -dlsctbenchmark -Q"TRUNCATE TABLE test.Errors"
sqlcmd -S(local)\sqlexpress2016 -dlsctbenchmark -Q"TRUNCATE TABLE test.Hosts"
sqlcmd -S(local)\sqlexpress2016 -dlsctbenchmark -Q"TRUNCATE TABLE test.Intervals"
sqlcmd -S(local)\sqlexpress2016 -dlsctbenchmark -Q"TRUNCATE TABLE test.Logins"
sqlcmd -S(local)\sqlexpress2016 -dlsctbenchmark -Q"TRUNCATE TABLE test.NormalizedQueries"
sqlcmd -S(local)\sqlexpress2016 -dlsctbenchmark -Q"TRUNCATE TABLE test.WaitStats"
sqlcmd -S(local) -dbenchmark_analysis -Q"TRUNCATE TABLE test.WorkloadDetails"
sqlcmd -S(local) -dbenchmark_analysis -Q"TRUNCATE TABLE benchmark"
start "" powershell -File .\generate-workload.ps1 -start 0
start "" powershell -File .\generate-workload.ps1 -start 1000000
start "" powershell -File .\generate-workload.ps1 -start 2000000
start "" powershell -File .\generate-workload.ps1 -start 3000000
start "" powershell -File .\generate-workload.ps1 -start 4000000
start "" powershell -File .\generate-workload.ps1 -start 5000000