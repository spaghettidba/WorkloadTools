CREATE TABLE IF NOT EXISTS [WorkloadDetails](
	[interval_id] INTEGER NOT NULL,

	[sql_hash] INTEGER NOT NULL,
	[application_id] INTEGER NOT NULL,
	[database_id] INTEGER NOT NULL,
	[host_id] INTEGER NOT NULL,
	[login_id] INTEGER NOT NULL,

	[avg_cpu_us] INTEGER NULL,
    [min_cpu_us] INTEGER NULL,
    [max_cpu_us] INTEGER NULL,
    [sum_cpu_us] INTEGER NULL,

	[avg_reads] INTEGER NULL,
    [min_reads] INTEGER NULL,
    [max_reads] INTEGER NULL,
    [sum_reads] INTEGER NULL,

	[avg_writes] INTEGER NULL,
    [min_writes] INTEGER NULL,
    [max_writes] INTEGER NULL,
    [sum_writes] INTEGER NULL,

	[avg_duration_us] INTEGER NULL,
    [min_duration_us] INTEGER NULL,
    [max_duration_us] INTEGER NULL,
    [sum_duration_us] INTEGER NULL,

    [execution_count] INTEGER NULL,

    CONSTRAINT PK_WorkloadDetails PRIMARY KEY CLUSTERED (
        [interval_id], 
        [sql_hash], 
        [application_id], 
        [database_id], 
        [host_id], 
        [login_id]
    )
)



CREATE TABLE IF NOT EXISTS [WorkloadSummary](
	[application_id] INTEGER NOT NULL,
	[database_id] INTEGER NOT NULL,
	[host_id] INTEGER NOT NULL,
	[login_id] INTEGER NOT NULL,
    *
    [min_cpu_us] INTEGER NULL,
    [max_cpu_us] INTEGER NULL,
    [sum_cpu_us] INTEGER NULL,

    [min_reads] INTEGER NULL,
    [max_reads] INTEGER NULL,
    [sum_reads] INTEGER NULL,

    [min_writes] INTEGER NULL,
    [max_writes] INTEGER NULL,
    [sum_writes] INTEGER NULL,

    [min_duration_us] INTEGER NULL,
    [max_duration_us] INTEGER NULL,
    [sum_duration_us] INTEGER NULL,

    [min_execution_date] TEXT,
    [max_execution_date] TEXT,

    [execution_count] INTEGER NULL,

    CONSTRAINT PK_WorkloadSummary PRIMARY KEY CLUSTERED (
        [application_id], 
        [database_id], 
        [host_id], 
        [login_id]
    )
)

CREATE TABLE IF NOT EXISTS [Applications](
	[application_id] INTEGER NOT NULL PRIMARY KEY,
	[application_name] TEXT NOT NULL
)

CREATE TABLE IF NOT EXISTS [Databases](
	[database_id] INTEGER NOT NULL PRIMARY KEY,
	[database_name] TEXT NOT NULL
)

CREATE TABLE IF NOT EXISTS [Hosts](
	[host_id] INTEGER NOT NULL PRIMARY KEY,
	[host_name] TEXT NOT NULL
)

CREATE TABLE IF NOT EXISTS [Logins](
	[login_id] INTEGER NOT NULL PRIMARY KEY,
	[login_name] TEXT NOT NULL
)

CREATE TABLE IF NOT EXISTS [Intervals] (
	[interval_id] INTEGER NOT NULL PRIMARY KEY,
	[end_time] TEXT NOT NULL,
	[duration_minutes] INTEGER NOT NULL
)

CREATE TABLE IF NOT EXISTS [NormalizedQueries](
	[sql_hash] INTEGER NOT NULL PRIMARY KEY,
	[normalized_text] TEXT NOT NULL,
    [example_text] TEXT NULL
)

CREATE TABLE IF NOT EXISTS [PerformanceCounters](
	[interval_id] INTEGER NOT NULL,
    [counter_name] TEXT NOT NULL,
    [min_counter_value] REAL NOT NULL,
    [max_counter_value] REAL NOT NULL,
    [avg_counter_value] REAL NOT NULL
)

CREATE TABLE IF NOT EXISTS [WaitStats](
	[interval_id] INTEGER NOT NULL,
    [wait_type] TEXT NOT NULL,
    [wait_sec] REAL NOT NULL,
    [resource_sec] REAL NOT NULL,
    [signal_sec] REAL NOT NULL,
    [wait_count] INTEGER NOT NULL
)

CREATE TABLE IF NOT EXISTS [DiskPerf] (
    [interval_id] INTEGER NOT NULL,
    [database_name] TEXT NULL,
    [physical_filename] TEXT NULL,
    [logical_filename] TEXT NULL,
    [file_type] TEXT NULL,
    [volume_mount_point] TEXT NULL,
    [read_latency_ms] INTEGER NULL,
    [reads] INTEGER NULL,
    [read_bytes] INTEGER NULL,
    [write_latency_ms] INTEGER NULL,
    [writes] INTEGER NULL,
    [write_bytes] INTEGER NULL,
    [cum_read_latency_ms] INTEGER NULL,
    [cum_reads] INTEGER NULL,
    [cum_read_bytes] INTEGER NULL,
    [cum_write_latency_ms] INTEGER NULL,
    [cum_writes] INTEGER NULL,
    [cum_write_bytes] INTEGER NULL
);

CREATE TABLE IF NOT EXISTS [Errors](
	[interval_id] INTEGER NOT NULL,
	[error_type] TEXT NOT NULL,
	[message] TEXT NULL,
	[error_count] INTEGER NULL
)
