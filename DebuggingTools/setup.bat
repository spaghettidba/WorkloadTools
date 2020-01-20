sqlcmd -S(local) -Q"CREATE DATABASE benchmark"
sqlcmd -S(local) -dbenchmark -Q"CREATE TABLE dbo.benchmark ( i int NULL )"
sqlcmd -S(local) -Q"CREATE DATABASE benchmark_analysis"

mkdir c:\temp
mkdir c:\temp\workloadtools
mkdir c:\temp\workloadtools\debug