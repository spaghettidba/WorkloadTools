CREATE PROCEDURE dbo.createAnalysisView 
	@baselineSchema AS nvarchar(max),
	@replaySchema AS nvarchar(max)
AS
BEGIN
	
	SET NOCOUNT ON;

	IF OBJECT_ID('dbo.WorkloadOverView') IS NOT NULL
		EXEC('DROP VIEW dbo.WorkloadOverView');
		

	DECLARE @sql nvarchar(max);
	DECLARE @sql_alldata nvarchar(max);

	SET @sql_alldata = N'IF OBJECT_ID(''[{0}].[AllData]'') IS NOT NULL DROP VIEW [{0}].[AllData]'
	SET @sql = REPLACE(@sql_alldata, '{0}', @baselineSchema);
	BEGIN TRY
		EXEC(@sql);
	END TRY
	BEGIN CATCH
		PRINT 'Unable to drop ' + @baselineSchema + '.AllData'
	END CATCH
	SET @sql = REPLACE(@sql_alldata, '{0}', @replaySchema);
	BEGIN TRY
		EXEC(@sql);
	END TRY
	BEGIN CATCH
		PRINT 'Unable to drop ' + @replaySchema + '.AllData'
	END CATCH

	SET @sql_alldata = N'
CREATE VIEW {0}.[AllData]
AS
SELECT 
	bWD.[interval_id], 
	bWD.[sql_hash], 
	bWD.[application_id], 
	bWD.[database_id], 
	bWD.[host_id], 
	bWD.[login_id], 
	bWD.[avg_cpu_ms], 
	bWD.[min_cpu_ms], 
	bWD.[max_cpu_ms], 
	bWD.[sum_cpu_ms], 
	bWD.[avg_reads], 
	bWD.[min_reads], 
	bWD.[max_reads], 
	bWD.[sum_reads], 
	bWD.[avg_writes], 
	bWD.[min_writes], 
	bWD.[max_writes], 
	bWD.[sum_writes], 
	bWD.[avg_duration_ms], 
	bWD.[min_duration_ms], 
	bWD.[max_duration_ms], 
	bWD.[sum_duration_ms], 
	bWD.[execution_count],
	bIn.duration_minutes, 
	bIn.end_time, 
	bNQ.normalized_text, 
	bNQ.example_text, 
	bAp.application_name, 
	bDB.database_name, 
	bHS.host_name, 
	bLI.login_name
FROM {0}.WorkloadDetails AS bWD
INNER JOIN {0}.Intervals AS bIn
	ON bIn.interval_id = bWD.interval_id
INNER JOIN {0}.NormalizedQueries AS bNQ
	ON bNQ.sql_hash = bWD.sql_hash
INNER JOIN {0}.Applications AS bAp
	ON bAp.application_id = bWD.application_id
INNER JOIN {0}.Databases AS bDB
	ON bDB.database_id = bWD.database_id
INNER JOIN {0}.Hosts AS bHS
	ON bHS.host_id = bWD.host_id
INNER JOIN {0}.Logins AS bLI
	ON bLI.login_id = bWD.login_id
'

	SET @sql = REPLACE(@sql_alldata, '{0}', @baselineSchema);

	BEGIN TRY
		EXEC(@sql);
	END TRY
	BEGIN CATCH
		PRINT 'Unable to create ' + @baselineSchema + '.AllData'
	END CATCH

	SET @sql = REPLACE(@sql_alldata, '{0}', @replaySchema);

	BEGIN TRY
		EXEC(@sql);
	END TRY
	BEGIN CATCH
		PRINT 'Unable to create ' + @replaySchema + '.AllData'
	END CATCH


	SET @sql = N'
CREATE VIEW [dbo].[WorkloadOverview]
AS
WITH BaselineData AS (
	SELECT 
		bWD.*, bIn.duration_minutes, bIn.end_time, 
		bNQ.normalized_text, bNQ.example_text, 
		bAp.application_name, bDB.database_name, 
		bHS.host_name, bLI.login_name
	FROM '+ @baselineSchema +'.WorkloadDetails AS bWD
	INNER JOIN '+ @baselineSchema +'.Intervals AS bIn
		ON bIn.interval_id = bWD.interval_id
	INNER JOIN '+ @baselineSchema +'.NormalizedQueries AS bNQ
		ON bNQ.sql_hash = bWD.sql_hash
	INNER JOIN '+ @baselineSchema +'.Applications AS bAp
		ON bAp.application_id = bWD.application_id
	INNER JOIN '+ @baselineSchema +'.Databases AS bDB
		ON bDB.database_id = bWD.database_id
	INNER JOIN '+ @baselineSchema +'.Hosts AS bHS
		ON bHS.host_id = bWD.host_id
	INNER JOIN '+ @baselineSchema +'.Logins AS bLI
		ON bLI.login_id = bWD.login_id
),
ReplayData AS (
	SELECT 
		rWD.*, rIn.duration_minutes, rIn.end_time, 
		rNQ.normalized_text, rNQ.example_text, 
		rAp.application_name, rDB.database_name, 
		rHS.host_name, rLI.login_name
	FROM '+ @replaySchema +'.WorkloadDetails AS rWD
	INNER JOIN '+ @replaySchema +'.Intervals AS rIn
		ON rIn.interval_id = rWD.interval_id
	INNER JOIN '+ @replaySchema +'.NormalizedQueries AS rNQ
		ON rNQ.sql_hash = rWD.sql_hash
	INNER JOIN '+ @replaySchema +'.Applications AS rAp
		ON rAp.application_id = rWD.application_id
	INNER JOIN '+ @replaySchema +'.Databases AS rDB
		ON rDB.database_id = rWD.database_id
	INNER JOIN '+ @replaySchema +'.Hosts AS rHS
		ON rHS.host_id = rWD.host_id
	INNER JOIN '+ @replaySchema +'.Logins AS rLI
		ON rLI.login_id = rWD.login_id
),
AllCorrelatedData AS (
	SELECT 
		b.interval_id, b.application_name, b.database_name, b.host_name, b.login_name, b.sql_hash, b.avg_cpu_ms, b.min_cpu_ms, b.max_cpu_ms, b.sum_cpu_ms, b.avg_reads, b.min_reads, b.max_reads, b.sum_reads, b.avg_writes, b.min_writes, b.max_writes, b.sum_writes, b.avg_duration_ms, b.min_duration_ms, b.max_duration_ms, b.sum_duration_ms, b.execution_count, b.duration_minutes, b.end_time, b.normalized_text, b.example_text,
		p.interval_id AS interval_id2, p.application_name AS application_name2, p.database_name AS database_name2, p.host_name AS host_name2, p.login_name AS login_name2, p.sql_hash AS sql_hash2, p.avg_cpu_ms AS avg_cpu_ms2, p.min_cpu_ms AS min_cpu_ms2, p.max_cpu_ms AS max_cpu_ms2, p.sum_cpu_ms AS sum_cpu_ms2, p.avg_reads AS avg_reads2, p.min_reads AS min_reads2, p.max_reads AS max_reads2, p.sum_reads AS sum_reads2, p.avg_writes AS avg_writes2, p.min_writes AS min_writes2, p.max_writes AS max_writes2, p.sum_writes AS sum_writes2, p.avg_duration_ms AS avg_duration_ms2, p.min_duration_ms AS min_duration_ms2, p.max_duration_ms AS max_duration_ms2, p.sum_duration_ms AS sum_duration_ms2, p.execution_count AS execution_count2, p.duration_minutes AS duration_minutes2, p.end_time AS end_time2
	FROM BaselineData AS b
	OUTER APPLY (
		SELECT *
		FROM ReplayData AS r
		WHERE r.sql_hash = b.sql_hash
			AND r.interval_id = (
				SELECT TOP(1) interval_id 
				FROM '+ @replaySchema +'.Intervals 
				WHERE end_time > b.end_time
				ORDER BY interval_id ASC
			)
	) AS p
)
SELECT interval_id, 
	end_time, 
	duration_minutes,
	sql_hash,
	example_text,
	normalized_text,
	application_name,
	database_name, 
	host_name, 
	login_name,
	AVG(avg_duration_ms) AS avg_duration_ms,
	MIN(min_duration_ms) AS min_duration_ms,
	MAX(max_duration_ms) AS max_duration_ms,
	SUM(sum_duration_ms) AS sum_duration_ms,
	AVG(avg_cpu_ms) AS avg_cpu_ms,
	MIN(min_cpu_ms) AS min_cpu_ms,
	MAX(max_cpu_ms) AS max_cpu_ms,
	SUM(sum_cpu_ms) AS sum_cpu_ms,
	AVG(avg_reads) AS avg_reads,
	MIN(min_reads) AS min_reads,
	MAX(max_reads) AS max_reads,
	SUM(sum_reads) AS sum_reads,
	AVG(avg_writes) AS avg_writes,
	MIN(min_writes) AS min_writes,
	MAX(max_writes) AS max_writes,
	SUM(sum_writes) AS sum_writes,
	SUM(execution_count) AS execution_count,

	AVG(avg_duration_ms2) AS avg_duration_ms2,
	MIN(min_duration_ms2) AS min_duration_ms2,
	MAX(max_duration_ms2) AS max_duration_ms2,
	SUM(sum_duration_ms2) AS sum_duration_ms2,
	SUM(execution_count2) AS execution_count2,
	AVG(avg_cpu_ms2) AS avg_cpu_ms2,
	MIN(min_cpu_ms2) AS min_cpu_ms2,
	MAX(max_cpu_ms2) AS max_cpu_ms2,
	SUM(sum_cpu_ms2) AS sum_cpu_ms2,
	AVG(avg_reads2) AS avg_reads2,
	MIN(min_reads2) AS min_reads2,
	MAX(max_reads2) AS max_reads2,
	SUM(sum_reads2) AS sum_reads2,
	AVG(avg_writes2) AS avg_writes2,
	MIN(min_writes2) AS min_writes2,
	MAX(max_writes2) AS max_writes2,
	SUM(sum_writes2) AS sum_writes2
FROM AllCorrelatedData
GROUP BY 
	interval_id, 
	end_time, 
	duration_minutes,
	sql_hash,
	example_text,
	normalized_text,
	application_name,
	database_name, 
	host_name, 
	login_name
'


	EXEC(@sql);

END

