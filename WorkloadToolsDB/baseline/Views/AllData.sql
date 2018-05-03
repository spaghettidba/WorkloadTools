
CREATE VIEW [baseline].[AllData]
AS
SELECT 
	bWD.*, bIn.duration_minutes, bIn.end_time, 
	bNQ.normalized_text, bNQ.example_text, 
	bAp.application_name, bDB.database_name, 
	bHS.host_name, bLI.login_name
FROM baseline.WorkloadDetails AS bWD
INNER JOIN baseline.Intervals AS bIn
	ON bIn.interval_id = bWD.interval_id
INNER JOIN baseline.NormalizedQueries AS bNQ
	ON bNQ.sql_hash = bWD.sql_hash
INNER JOIN baseline.Applications AS bAp
	ON bAp.application_id = bWD.application_id
INNER JOIN baseline.Databases AS bDB
	ON bDB.database_id = bWD.database_id
INNER JOIN baseline.Hosts AS bHS
	ON bHS.host_id = bWD.host_id
INNER JOIN baseline.Logins AS bLI
	ON bLI.login_id = bWD.login_id