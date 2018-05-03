
CREATE VIEW [dbo].[AllCorrelatedData]
AS
SELECT 
	b.interval_id, b.application_name, b.database_name, b.host_name, b.login_name, b.sql_hash, b.avg_cpu_ms, b.min_cpu_ms, b.max_cpu_ms, b.sum_cpu_ms, b.avg_reads, b.min_reads, b.max_reads, b.sum_reads, b.avg_writes, b.min_writes, b.max_writes, b.sum_writes, b.avg_duration_ms, b.min_duration_ms, b.max_duration_ms, b.sum_duration_ms, b.execution_count, b.duration_minutes, b.end_time, b.normalized_text, b.example_text,
	p.interval_id AS interval_id2, p.application_name AS application_name2, p.database_name AS database_name2, p.host_name AS host_name2, p.login_name AS login_name2, p.sql_hash AS sql_hash2, p.avg_cpu_ms AS avg_cpu_ms2, p.min_cpu_ms AS min_cpu_ms2, p.max_cpu_ms AS max_cpu_ms2, p.sum_cpu_ms AS sum_cpu_ms2, p.avg_reads AS avg_reads2, p.min_reads AS min_reads2, p.max_reads AS max_reads2, p.sum_reads AS sum_reads2, p.avg_writes AS avg_writes2, p.min_writes AS min_writes2, p.max_writes AS max_writes2, p.sum_writes AS sum_writes2, p.avg_duration_ms AS avg_duration_ms2, p.min_duration_ms AS min_duration_ms2, p.max_duration_ms AS max_duration_ms2, p.sum_duration_ms AS sum_duration_ms2, p.execution_count AS execution_count2, p.duration_minutes AS duration_minutes2, p.end_time AS end_time2
FROM baseline.AllData AS b
OUTER APPLY (
	SELECT *
	FROM replay.AllData AS r
	WHERE r.sql_hash = b.sql_hash
		AND r.interval_id = (
			SELECT TOP(1) interval_id 
			FROM replay.Intervals 
			WHERE end_time > b.end_time
			ORDER BY interval_id ASC
		)
) AS p