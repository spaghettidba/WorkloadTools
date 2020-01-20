sqlcmd -S(local) -Q"CREATE DATABASE benchmark"
sqlcmd -S(local) -dbenchmark -Q"CREATE TABLE dbo.benchmark ( i int NULL )"
sqlcmd -S(local) -Q"CREATE DATABASE benchmark_analysis"