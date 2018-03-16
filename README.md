# WorkloadTools

*WorkloadTools is a collection of tools to collect, analyze and replay workloads on a SQL Server instance.*

SqlWorkload is a command line tool to start workload collection, analyze the collected data and replay the workload to a target machine, all in REALTIME.

*Command line switches*

`--ListenerType` `SqlTraceWorkloadListener | ProfilerWorkloadListener | ExtendedEventsWorkloadListener`

`--Source` Path to the source of the workload capture. Can be a trace definition (`Listener\sqlworkload.tdf`) or a trace script (`Listener\sqlworkload.sql`) 

`--SourceServerName` Name of the source SQL Server instance

`--SourceUserName` User name to connect to SQL Server with SQL authentication. Will use Windows authentication if empty or missing.

`--SourcePassword` Password

`--TargetServerName` Name of the target SQL Server instance

`--TargetUserName` User name to connect to SQL Server with SQL authentication. Will use Windows authentication if empty or missing.

`--TargetPassword` Password

`--ApplicationFilter` Name of a single application to filter. Prepend the "^" character to exclude the value (e.g. "^sqlcmd.exe" excludes sqlcmd.exe)

`--DatabaseFilter` Name of a single database to filter. Prepend the "^" character to exclude the value.

`--HostFilter` Name of a single host to filter. Prepend the "^" character to exclude the value.

`--LoginFilter` Name of a single login to filter. Prepend the "^" character to exclude the value.

`--StatsServer` Name of the SQL Server instance to use to log the statistics

`--StatsDatabase` Name of the database to store the statistics

`--StatsSchema` Name of the schema to store the statistics. If missing, the schema will be created.

`--StatsInterval` Interval in minutes between each dump of the workload statistics

`--StatsUserName` Username to authenticate to the statistics database 

`--StatsPassword` Password to authenticate to the statistics database


# Example:

```
--ListenerType SqlTraceWorkloadListener --Source Listener\sqlworkload.sql --SourceServerName SQLDEMO\SQL2014 --SourceUserName sa --SourcePassword P4$$w0rd! --TargetServerName SQLDEMO\SQL2016 --TargetUserName sa --TargetPassword P4$$w0rd! --DatabaseFilter DS3 --StatsServer SQLDEMO\SQL2014 --StatsDatabase RTR --StatsInterval 1 --StatsUserName sa --StatsPassword P4$$w0rd!
```