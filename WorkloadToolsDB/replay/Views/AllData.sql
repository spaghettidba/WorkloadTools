
CREATE VIEW [replay].[AllData]
AS
SELECT 
	rWD.*, rIn.duration_minutes, rIn.end_time, 
	rNQ.normalized_text, rNQ.example_text, 
	rAp.application_name, rDB.database_name, 
	rHS.host_name, rLI.login_name
FROM replay.WorkloadDetails AS rWD
INNER JOIN replay.Intervals AS rIn
	ON rIn.interval_id = rWD.interval_id
INNER JOIN replay.NormalizedQueries AS rNQ
	ON rNQ.sql_hash = rWD.sql_hash
INNER JOIN replay.Applications AS rAp
	ON rAp.application_id = rWD.application_id
INNER JOIN replay.Databases AS rDB
	ON rDB.database_id = rWD.database_id
INNER JOIN replay.Hosts AS rHS
	ON rHS.host_id = rWD.host_id
INNER JOIN replay.Logins AS rLI
	ON rLI.login_id = rWD.login_id