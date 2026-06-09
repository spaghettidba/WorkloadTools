CREATE SCHEMA IF NOT EXISTS "{SchemaName}";

CREATE TABLE IF NOT EXISTS "{SchemaName}"."WorkloadDetails" (
	"interval_id" INTEGER NOT NULL,

	"sql_hash" BIGINT NOT NULL,
	"application_id" INTEGER NOT NULL,
	"database_id" INTEGER NOT NULL,
	"host_id" INTEGER NOT NULL,
	"login_id" INTEGER NOT NULL,

	"avg_cpu_us" BIGINT NULL,
    "min_cpu_us" BIGINT NULL,
    "max_cpu_us" BIGINT NULL,
    "sum_cpu_us" BIGINT NULL,

	"avg_reads" BIGINT NULL,
    "min_reads" BIGINT NULL,
    "max_reads" BIGINT NULL,
    "sum_reads" BIGINT NULL,

	"avg_writes" BIGINT NULL,
    "min_writes" BIGINT NULL,
    "max_writes" BIGINT NULL,
    "sum_writes" BIGINT NULL,

	"avg_duration_us" BIGINT NULL,
    "min_duration_us" BIGINT NULL,
    "max_duration_us" BIGINT NULL,
    "sum_duration_us" BIGINT NULL,

    "execution_count" BIGINT NULL,

    CONSTRAINT PK_WorkloadDetails PRIMARY KEY (
        "interval_id", 
        "sql_hash", 
        "application_id", 
        "database_id", 
        "host_id", 
        "login_id"
    )
);




CREATE TABLE IF NOT EXISTS "{SchemaName}"."WorkloadSummary"(
	"application_id" INTEGER NOT NULL,
	"database_id" INTEGER NOT NULL,
	"host_id" INTEGER NOT NULL,
	"login_id" INTEGER NOT NULL,
    
    "min_cpu_us" BIGINT NULL,
    "max_cpu_us" BIGINT NULL,
    "sum_cpu_us" BIGINT NULL,

    "min_reads" BIGINT NULL,
    "max_reads" BIGINT NULL,
    "sum_reads" BIGINT NULL,

    "min_writes" BIGINT NULL,
    "max_writes" BIGINT NULL,
    "sum_writes" BIGINT NULL,

    "min_duration_us" BIGINT NULL,
    "max_duration_us" BIGINT NULL,
    "sum_duration_us" BIGINT NULL,

    "min_execution_date" TIMESTAMP,
    "max_execution_date" TIMESTAMP,

    "execution_count" BIGINT NULL,

    CONSTRAINT PK_WorkloadSummary PRIMARY KEY (
        "application_id", 
        "database_id", 
        "host_id", 
        "login_id"
    )
);

CREATE TABLE IF NOT EXISTS "{SchemaName}"."Applications"(
	"application_id" INTEGER NOT NULL PRIMARY KEY,
	"application_name" TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "{SchemaName}"."Databases"(
	"database_id" INTEGER NOT NULL PRIMARY KEY,
	"database_name" TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "{SchemaName}"."Hosts"(
	"host_id" INTEGER NOT NULL PRIMARY KEY,
	"host_name" TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "{SchemaName}"."Logins"(
	"login_id" INTEGER NOT NULL PRIMARY KEY,
	"login_name" TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "{SchemaName}"."Intervals" (
	"interval_id" INTEGER NOT NULL PRIMARY KEY,
	"end_time" TIMESTAMP NOT NULL,
	"duration_minutes" INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS "{SchemaName}"."NormalizedQueries"(
	"sql_hash" BIGINT NOT NULL PRIMARY KEY,
	"normalized_text" TEXT NOT NULL,
    "example_text" TEXT NULL
);

CREATE TABLE IF NOT EXISTS "{SchemaName}"."PerformanceCounters"(
	"interval_id" INTEGER NOT NULL,
    "counter_name" TEXT NOT NULL,
    "min_counter_value" REAL NOT NULL,
    "max_counter_value" REAL NOT NULL,
    "avg_counter_value" REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS "{SchemaName}"."WaitStats"(
	"interval_id" INTEGER NOT NULL,
    "wait_type" TEXT NOT NULL,
    "wait_sec" REAL NOT NULL,
    "resource_sec" REAL NOT NULL,
    "signal_sec" REAL NOT NULL,
    "wait_count" BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS "{SchemaName}"."DiskPerf" (
    "interval_id" INTEGER NOT NULL,
    "database_name" TEXT NULL,
    "physical_filename" TEXT NULL,
    "logical_filename" TEXT NULL,
    "file_type" TEXT NULL,
    "volume_mount_point" TEXT NULL,
    "read_latency_ms" BIGINT NULL,
    "reads" BIGINT NULL,
    "read_bytes" BIGINT NULL,
    "write_latency_ms" BIGINT NULL,
    "writes" BIGINT NULL,
    "write_bytes" BIGINT NULL,
    "cum_read_latency_ms" BIGINT NULL,
    "cum_reads" BIGINT NULL,
    "cum_read_bytes" BIGINT NULL,
    "cum_write_latency_ms" BIGINT NULL,
    "cum_writes" BIGINT NULL,
    "cum_write_bytes" BIGINT NULL
);

CREATE TABLE IF NOT EXISTS "{SchemaName}"."Errors"(
	"interval_id" INTEGER NOT NULL,
	"error_type" TEXT NOT NULL,
	"message" TEXT NULL,
	"error_count" INTEGER NULL
);
