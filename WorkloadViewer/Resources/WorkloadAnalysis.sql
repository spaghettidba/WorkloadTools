WITH baseData AS (
	SELECT 
		DATEDIFF(minute, Base.end_time, bIn.end_time) AS offset_minutes,
		bWD.sql_hash, 
		bWD.avg_cpu_us, 
		bWD.min_cpu_us, 
		bWD.max_cpu_us, 
		bWD.sum_cpu_us, 
		bWD.avg_reads, 
		bWD.min_reads, 
		bWD.max_reads, 
		bWD.sum_reads, 
		bWD.avg_writes, 
		bWD.min_writes, 
		bWD.max_writes, 
		bWD.sum_writes, 
		bWD.avg_duration_us, 
		bWD.min_duration_us, 
		bWD.max_duration_us, 
		bWD.sum_duration_us, 
		bWD.execution_count,
		bIn.duration_minutes, 
		bNQ.normalized_text, 
		bNQ.example_text, 
		bAp.application_name, 
		bDB.database_name, 
		bHS.host_name, 
		bLI.login_name
	FROM capture.WorkloadDetails AS bWD
	INNER JOIN capture.Intervals AS bIn
		ON bIn.interval_id = bWD.interval_id
	INNER JOIN capture.NormalizedQueries AS bNQ
		ON bNQ.sql_hash = bWD.sql_hash
	INNER JOIN capture.Applications AS bAp
		ON bAp.application_id = bWD.application_id
	INNER JOIN capture.Databases AS bDB
		ON bDB.database_id = bWD.database_id
	INNER JOIN capture.Hosts AS bHS
		ON bHS.host_id = bWD.host_id
	INNER JOIN capture.Logins AS bLI
		ON bLI.login_id = bWD.login_id
	CROSS APPLY (
		SELECT TOP(1) base.end_time
		FROM capture.Intervals AS base
		ORDER BY interval_id
	) AS Base
)
SELECT 	(offset_minutes / preaggregation) * preaggregation AS offset_minutes,
		preaggregation AS duration_minutes,
		sql_hash,
		application_name, 
		database_name, 
		host_name, 
		login_name,
		AVG(avg_cpu_us)      AS avg_cpu_us, 
		MIN(min_cpu_us)      AS min_cpu_us, 
		MAX(max_cpu_us)      AS max_cpu_us, 
		SUM(sum_cpu_us)      AS sum_cpu_us, 
		AVG(avg_reads)       AS avg_reads, 
		MIN(min_reads)       AS min_reads, 
		MAX(max_reads)       AS max_reads, 
		SUM(sum_reads)       AS sum_reads, 
		AVG(avg_writes)      AS avg_writes, 
		MIN(min_writes)      AS min_writes, 
		MAX(max_writes)      AS max_writes, 
		SUM(sum_writes)      AS sum_writes, 
		AVG(avg_duration_us) AS avg_duration_us, 
		MIN(min_duration_us) AS min_duration_us, 
		MAX(max_duration_us) AS max_duration_us, 
		SUM(sum_duration_us) AS sum_duration_us, 
		SUM(execution_count) AS execution_count
FROM baseData
GROUP BY 
		(offset_minutes / preaggregation) * preaggregation,
		duration_minutes % preaggregation,
		sql_hash,
		application_name, 
		database_name, 
		host_name, 
		login_name
ORDER BY 
		offset_minutes;