IF SCHEMA_ID('{SchemaName}') IS NULL
    EXEC('CREATE SCHEMA {SchemaName}');

IF OBJECT_ID('{SchemaName}.WorkloadDetails') IS NULL

CREATE TABLE [{SchemaName}].[WorkloadDetails](
	[interval_id] [int] NOT NULL,

	[sql_hash] [bigint] NOT NULL,
	[application_id] [int] NOT NULL,
	[database_id] [int] NOT NULL,
	[host_id] [int] NOT NULL,
	[login_id] [int] NOT NULL,

	[avg_cpu_us] [bigint] NULL,
    [min_cpu_us] [bigint] NULL,
    [max_cpu_us] [bigint] NULL,
    [sum_cpu_us] [bigint] NULL,

	[avg_reads] [bigint] NULL,
    [min_reads] [bigint] NULL,
    [max_reads] [bigint] NULL,
    [sum_reads] [bigint] NULL,

	[avg_writes] [bigint] NULL,
    [min_writes] [bigint] NULL,
    [max_writes] [bigint] NULL,
    [sum_writes] [bigint] NULL,

	[avg_duration_us] [bigint] NULL,
    [min_duration_us] [bigint] NULL,
    [max_duration_us] [bigint] NULL,
    [sum_duration_us] [bigint] NULL,

    [execution_count] [bigint] NULL,

    CONSTRAINT PK_WorkloadDetails PRIMARY KEY CLUSTERED (
        [interval_id], 
        [sql_hash], 
        [application_id], 
        [database_id], 
        [host_id], 
        [login_id]
    )
)


IF OBJECT_ID('{SchemaName}.Applications') IS NULL

CREATE TABLE [{SchemaName}].[Applications](
	[application_id] [int] NOT NULL PRIMARY KEY,
	[application_name] [nvarchar](128) NOT NULL
)

IF OBJECT_ID('{SchemaName}.Databases') IS NULL

CREATE TABLE [{SchemaName}].[Databases](
	[database_id] [int] NOT NULL PRIMARY KEY,
	[database_name] [nvarchar](128) NOT NULL
)

IF OBJECT_ID('{SchemaName}.Hosts') IS NULL

CREATE TABLE [{SchemaName}].[Hosts](
	[host_id] [int] NOT NULL PRIMARY KEY,
	[host_name] [nvarchar](128) NOT NULL
)

IF OBJECT_ID('{SchemaName}.Logins') IS NULL

CREATE TABLE [{SchemaName}].[Logins](
	[login_id] [int] NOT NULL PRIMARY KEY,
	[login_name] [nvarchar](128) NOT NULL
)

IF OBJECT_ID('{SchemaName}.Intervals') IS NULL

CREATE TABLE [{SchemaName}].[Intervals] (
	[interval_id] [int] NOT NULL PRIMARY KEY,
	[end_time] [datetime] NOT NULL,
	[duration_minutes] [int] NOT NULL
)

IF OBJECT_ID('{SchemaName}.NormalizedQueries') IS NULL

CREATE TABLE [{SchemaName}].[NormalizedQueries](
	[sql_hash] [bigint] NOT NULL PRIMARY KEY,
	[normalized_text] [nvarchar](max) NOT NULL,
    [example_text] [nvarchar](max) NULL
)

IF OBJECT_ID('{SchemaName}.PerformanceCounters') IS NULL

CREATE TABLE [{SchemaName}].[PerformanceCounters](
	[interval_id] [int] NOT NULL,
    [counter_name] [varchar](255) NOT NULL,
    [min_counter_value] [float] NOT NULL,
    [max_counter_value] [float] NOT NULL,
    [avg_counter_value] [float] NOT NULL
)

IF OBJECT_ID('{SchemaName}.WaitStats') IS NULL

CREATE TABLE [{SchemaName}].[WaitStats](
	[interval_id] [int] NOT NULL,
    [wait_type] [varchar](255) NOT NULL,
    [wait_sec] [float] NOT NULL,
    [resource_sec] [float] NOT NULL,
    [signal_sec] [float] NOT NULL,
    [wait_count] [bigint] NOT NULL
)


IF OBJECT_ID('{SchemaName}.Errors') IS NULL

CREATE TABLE [{SchemaName}].[Errors](
	[interval_id] [int] NOT NULL,
	[error_type] [nvarchar](30) NOT NULL,
	[message] [nvarchar](max) NULL,
	[error_count] int NULL
)
