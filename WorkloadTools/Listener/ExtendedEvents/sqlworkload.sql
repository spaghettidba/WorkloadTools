IF EXISTS (
	SELECT *
	FROM sys.dm_xe_sessions
	WHERE name = 'sqlworkload'
)
BEGIN
    ALTER EVENT SESSION [sqlworkload] ON SERVER STATE = STOP;
    DROP EVENT SESSION [sqlworkload] ON SERVER;
END

CREATE EVENT SESSION [sqlworkload] ON SERVER 
ADD EVENT sqlserver.attention (
	ACTION(	
		package0.event_sequence, 
		sqlserver.client_app_name, 
		sqlserver.client_hostname, 
		sqlserver.database_id, 
		sqlserver.database_name, 
		sqlserver.is_system, 
		sqlserver.server_principal_name, 
		sqlserver.session_id, 
		sqlserver.sql_text
	)
	{0}
),
ADD EVENT sqlserver.rpc_completed (
	SET collect_data_stream = (0),
	collect_output_parameters = (1),
	collect_statement = (1) 
	ACTION(
		package0.event_sequence, 
		sqlserver.client_app_name, 
		sqlserver.client_hostname, 
		sqlserver.database_id, 
		sqlserver.database_name, 
		sqlserver.is_system, 
		sqlserver.server_principal_name, 
		sqlserver.session_id
	) 
	{0}
),
ADD EVENT sqlserver.sql_batch_completed (
	SET collect_batch_text = (1) 
	ACTION(
		package0.event_sequence, 
		sqlserver.client_app_name, 
		sqlserver.client_hostname, 
		sqlserver.database_id, 
		sqlserver.database_name, 
		sqlserver.is_system, 
		sqlserver.server_principal_name, 
		sqlserver.session_id
	) 
	{0}
)
WITH (
	MAX_MEMORY = 40960 KB,
	EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,
	MAX_DISPATCH_LATENCY = 30 SECONDS,
	MAX_EVENT_SIZE = 0 KB,
	MEMORY_PARTITION_MODE = PER_CPU,
	TRACK_CAUSALITY = OFF,
	STARTUP_STATE = OFF
);


ALTER EVENT SESSION [sqlworkload] ON SERVER STATE = START;