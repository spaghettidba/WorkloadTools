CREATE PROCEDURE [dbo].[createAnalysisView]
	@baselineSchema AS nvarchar(max),
	@replaySchema AS nvarchar(max)
AS
BEGIN
	
SET NOCOUNT ON;

DECLARE @sql nvarchar(max);

-- DROP PowerBI_WaitStats
IF OBJECT_ID( QUOTENAME(@baselineSchema) +'.'+ QUOTENAME('PowerBI_WaitStats') ) IS NOT NULL
BEGIN
    SET @sql = 'DROP VIEW ' + QUOTENAME(@baselineSchema) +'.'+ QUOTENAME('PowerBI_WaitStats')
    EXEC(@sql)
END
IF OBJECT_ID( QUOTENAME(@replaySchema) +'.'+ QUOTENAME('PowerBI_WaitStats') ) IS NOT NULL
BEGIN
    SET @sql = 'DROP VIEW ' + QUOTENAME(@replaySchema) +'.'+ QUOTENAME('PowerBI_WaitStats')
    EXEC(@sql)
END

-- DROP PowerBI_WinPerfCounters
IF OBJECT_ID( QUOTENAME(@baselineSchema) +'.'+ QUOTENAME('PowerBI_WinPerfCounters') ) IS NOT NULL
BEGIN
    SET @sql = 'DROP VIEW ' + QUOTENAME(@baselineSchema) +'.'+ QUOTENAME('PowerBI_WinPerfCounters')
    EXEC(@sql)
END
IF OBJECT_ID( QUOTENAME(@replaySchema) +'.'+ QUOTENAME('PowerBI_WinPerfCounters') ) IS NOT NULL
BEGIN
    SET @sql = 'DROP VIEW ' + QUOTENAME(@replaySchema) +'.'+ QUOTENAME('PowerBI_WinPerfCounters')
    EXEC(@sql)
END

-- DROP PowerBI_WorkloadData
IF OBJECT_ID( QUOTENAME(@baselineSchema) +'.'+ QUOTENAME('PowerBI_WorkloadData') ) IS NOT NULL
BEGIN
    SET @sql = 'DROP VIEW ' + QUOTENAME(@baselineSchema) +'.'+ QUOTENAME('PowerBI_WorkloadData')
    EXEC(@sql)
END
IF OBJECT_ID( QUOTENAME(@replaySchema) +'.'+ QUOTENAME('PowerBI_WorkloadData') ) IS NOT NULL
BEGIN
    SET @sql = 'DROP VIEW ' + QUOTENAME(@replaySchema) +'.'+ QUOTENAME('PowerBI_WorkloadData')
    EXEC(@sql)
END

-- DROP PowerBI_WorkloadQueries
IF OBJECT_ID( QUOTENAME(@baselineSchema) +'.'+ QUOTENAME('PowerBI_WorkloadQueries') ) IS NOT NULL
BEGIN
    SET @sql = 'DROP VIEW ' + QUOTENAME(@baselineSchema) +'.'+ QUOTENAME('PowerBI_WorkloadQueries')
    EXEC(@sql)
END
IF OBJECT_ID( QUOTENAME(@replaySchema) +'.'+ QUOTENAME('PowerBI_WorkloadQueries') ) IS NOT NULL
BEGIN
    SET @sql = 'DROP VIEW ' + QUOTENAME(@replaySchema) +'.'+ QUOTENAME('PowerBI_WorkloadQueries')
    EXEC(@sql)
END

-- DROP PowerBI_Time
IF OBJECT_ID( QUOTENAME(@baselineSchema) +'.'+ QUOTENAME('PowerBI_Time') ) IS NOT NULL
BEGIN
    SET @sql = 'DROP VIEW ' + QUOTENAME(@baselineSchema) +'.'+ QUOTENAME('PowerBI_Time')
    EXEC(@sql)
END
IF OBJECT_ID( QUOTENAME(@replaySchema) +'.'+ QUOTENAME('PowerBI_Time') ) IS NOT NULL
BEGIN
    SET @sql = 'DROP VIEW ' + QUOTENAME(@replaySchema) +'.'+ QUOTENAME('PowerBI_Time')
    EXEC(@sql)
END


-- CREATE VIEWS
--===========================================================
DECLARE @PowerBI_WorkloadQueries nvarchar(max) = N'
CREATE VIEW {0}.[PowerBI_WorkloadQueries] 
AS
SELECT
	bNQ.[sql_hash] AS [Sql Hash],
	bNQ.[normalized_text] AS [Sql Normalized Text], 
	bNQ.[example_text] AS [Sql Sample Text]
FROM {0}.[NormalizedQueries] AS bNQ
'

IF @baselineSchema IS NOT NULL
	BEGIN
		SET @sql = REPLACE(@PowerBI_WorkloadQueries, '{0}', @baselineSchema);
		EXEC(@sql);
	END
IF @replaySchema IS NOT NULL
	BEGIN
		SET @sql = REPLACE(@PowerBI_WorkloadQueries, '{0}', @replaySchema);
		EXEC(@sql);
	END

--===========================================================
DECLARE @PowerBI_WinPerfCounters nvarchar(max) = N'
CREATE VIEW {0}.[PowerBI_WinPerfCounters]
AS
SELECT
    bPC.[counter_name] AS [Win Counter]
    ,bPC.[min_counter_value] AS [Counter Min Value]
    ,bPC.[max_counter_value] AS [Counter Max Value]
    ,bPC.[avg_counter_value] AS [Counter Average Value]
    ,bIn.[Elapsed Time (min)]
FROM {0}.[PerformanceCounters] AS bPC
INNER JOIN (
    SELECT
        [interval_id],
        [duration_minutes],
        [end_time],
        SUM([duration_minutes]) OVER(ORDER BY [end_time] ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS [Elapsed Time (min)]
    FROM {0}.[Intervals]
) AS bIn
    ON bPC.[interval_id] = bIn.[interval_id]
'

IF @baselineSchema IS NOT NULL
	BEGIN
		SET @sql = REPLACE(@PowerBI_WinPerfCounters, '{0}', @baselineSchema);
		EXEC(@sql);
	END
IF @replaySchema IS NOT NULL
	BEGIN
		SET @sql = REPLACE(@PowerBI_WinPerfCounters, '{0}', @replaySchema);
		EXEC(@sql);
	END

--===========================================================
DECLARE @PowerBI_WorkloadData nvarchar(max) = N'
CREATE VIEW {0}.[PowerBI_WorkloadData]
AS
SELECT
	bWD.[sql_hash] AS [Sql Hash],
	bIn.[duration_minutes] AS [Interval Duration (min)], 
	bIn.[end_time] AS [Interval End Time],
	bIn.[Elapsed Time (min)],
	bAp.[application_name] AS [Application], 
	bDB.[database_name] AS [Database], 
	bHS.[host_name] AS [Host], 
	bLI.[login_name] AS [Login],
	bWD.[avg_cpu_us] AS [Avg Cpu (µs)], 
	bWD.[min_cpu_us] AS [Min Cpu (µs)], 
	bWD.[max_cpu_us] AS [Max Cpu (µs)], 
	bWD.[sum_cpu_us] AS [Sum Cpu (µs)], 
	bWD.[avg_reads] AS [Avg Reads], 
	bWD.[min_reads] AS [Min Reads], 
	bWD.[max_reads] AS [Max Reads], 
	bWD.[sum_reads] AS [Sum Reads], 
	bWD.[avg_writes] AS [Avg Writes], 
	bWD.[min_writes] AS [Min Writes], 
	bWD.[max_writes] AS [Max Writes], 
	bWD.[sum_writes] AS [Sum Writes], 
	bWD.[avg_duration_us] AS [Avg Duration (µs)], 
	bWD.[min_duration_us] AS [Min Duration (µs)], 
	bWD.[max_duration_us] AS [Max Duration (µs)], 
	bWD.[sum_duration_us] AS [Sum Duration (µs)], 
	bWD.[execution_count] AS [Execution Count]
FROM {0}.WorkloadDetails AS bWD
INNER JOIN (
	SELECT
		[interval_id],
		[duration_minutes],
		[end_time],
		SUM([duration_minutes]) OVER(ORDER BY [end_time] ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS [Elapsed Time (min)]
	FROM {0}.[Intervals]
) AS bIn
	ON bIn.[interval_id] = bWD.[interval_id]
INNER JOIN {0}.Applications AS bAp
	ON bAp.[application_id] = bWD.[application_id]
INNER JOIN {0}.Databases AS bDB
	ON bDB.[database_id] = [bWD].database_id
INNER JOIN {0}.Hosts AS bHS
	ON bHS.[host_id] = bWD.[host_id]
INNER JOIN {0}.Logins AS bLI
	ON bLI.[login_id] = bWD.[login_id]
'

IF @baselineSchema IS NOT NULL
	BEGIN
		SET @sql = REPLACE(@PowerBI_WorkloadData, '{0}', @baselineSchema);
		EXEC(@sql);
	END
IF @replaySchema IS NOT NULL
	BEGIN
		SET @sql = REPLACE(@PowerBI_WorkloadData, '{0}', @replaySchema);
		EXEC(@sql);
	END

--===========================================================
DECLARE @PowerBI_WaitStats nvarchar(max) = N'
CREATE VIEW {0}.[PowerBI_WaitStats]
AS
SELECT
	 bWS.[interval_id]
	,bWS.[wait_type] AS [Wait Type]
	,bWS.[wait_sec]*1000 AS [Wait Time (µs)]
	,bWS.[resource_sec]*1000 AS [Wait Time Resource (µs)]
	,bWS.[signal_sec]*1000 AS [Wait Time Signal (µs)]
	,bWS.[wait_count] AS [Wait Count]
	,bIn.[Elapsed Time (min)]
	,COALESCE(WsCat.[Wait Category],''<n/a>'') AS [Wait Type Category]
FROM {0}.[WaitStats] AS bWS
INNER JOIN (
	SELECT
		[interval_id],
		[duration_minutes],
		[end_time],
		SUM([duration_minutes]) OVER(ORDER BY [end_time] ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS [Elapsed Time (min)]
	FROM {0}.[Intervals]
) AS bIn
	ON bIn.[interval_id] = bWS.[interval_id]
LEFT JOIN (
VALUES 
	 (''HADR_AG_MUTEX'',''Replication'',0)
	,(''HADR_AR_CRITICAL_SECTION_ENTRY'',''Replication'',0)
	,(''HADR_AR_MANAGER_MUTEX'',''Replication'',0)
	,(''HADR_AR_UNLOAD_COMPLETED'',''Replication'',0)
	,(''HADR_ARCONTROLLER_NOTIFICATIONS_SUBSCRIBER_LIST'',''Replication'',0)
	,(''HADR_BACKUP_BULK_LOCK'',''Replication'',0)
	,(''HADR_BACKUP_QUEUE'',''Replication'',0)
	,(''HADR_CLUSAPI_CALL'',''Replication'',0)
	,(''HADR_COMPRESSED_CACHE_SYNC'',''Replication'',0)
	,(''HADR_CONNECTIVITY_INFO'',''Replication'',0)
	,(''HADR_DATABASE_FLOW_CONTROL'',''Replication'',0)
	,(''HADR_DATABASE_VERSIONING_STATE'',''Replication'',0)
	,(''HADR_DATABASE_WAIT_FOR_RECOVERY'',''Replication'',0)
	,(''HADR_DATABASE_WAIT_FOR_RESTART'',''Replication'',0)
	,(''HADR_DATABASE_WAIT_FOR_TRANSITION_TO_VERSIONING'',''Replication'',0)
	,(''HADR_DB_COMMAND'',''Replication'',0)
	,(''HADR_DB_OP_COMPLETION_SYNC'',''Replication'',0)
	,(''HADR_DB_OP_START_SYNC'',''Replication'',0)
	,(''HADR_DBR_SUBSCRIBER'',''Replication'',0)
	,(''HADR_DBR_SUBSCRIBER_FILTER_LIST'',''Replication'',0)
	,(''HADR_DBSEEDING'',''Replication'',0)
	,(''HADR_DBSEEDING_LIST'',''Replication'',0)
	,(''HADR_DBSTATECHANGE_SYNC'',''Replication'',0)
	,(''HADR_FABRIC_CALLBACK'',''Replication'',0)
	,(''HADR_FILESTREAM_BLOCK_FLUSH'',''Replication'',0)
	,(''HADR_FILESTREAM_FILE_CLOSE'',''Replication'',0)
	,(''HADR_FILESTREAM_FILE_REQUEST'',''Replication'',0)
	,(''HADR_FILESTREAM_IOMGR'',''Replication'',0)
	,(''HADR_FILESTREAM_IOMGR_IOCOMPLETION'',''Replication'',0)
	,(''HADR_FILESTREAM_MANAGER'',''Replication'',0)
	,(''HADR_FILESTREAM_PREPROC'',''Replication'',0)
	,(''HADR_GROUP_COMMIT'',''Replication'',0)
	,(''HADR_LOGCAPTURE_SYNC'',''Replication'',0)
	,(''HADR_LOGCAPTURE_WAIT'',''Replication'',0)
	,(''HADR_LOGPROGRESS_SYNC'',''Replication'',0)
	,(''HADR_NOTIFICATION_DEQUEUE'',''Replication'',0)
	,(''HADR_NOTIFICATION_WORKER_EXCLUSIVE_ACCESS'',''Replication'',0)
	,(''HADR_NOTIFICATION_WORKER_STARTUP_SYNC'',''Replication'',0)
	,(''HADR_NOTIFICATION_WORKER_TERMINATION_SYNC'',''Replication'',0)
	,(''HADR_PARTNER_SYNC'',''Replication'',0)
	,(''HADR_READ_ALL_NETWORKS'',''Replication'',0)
	,(''HADR_RECOVERY_WAIT_FOR_CONNECTION'',''Replication'',0)
	,(''HADR_RECOVERY_WAIT_FOR_UNDO'',''Replication'',0)
	,(''HADR_REPLICAINFO_SYNC'',''Replication'',0)
	,(''HADR_SEEDING_CANCELLATION'',''Replication'',0)
	,(''HADR_SEEDING_FILE_LIST'',''Replication'',0)
	,(''HADR_SEEDING_LIMIT_BACKUPS'',''Replication'',0)
	,(''HADR_SEEDING_SYNC_COMPLETION'',''Replication'',0)
	,(''HADR_SEEDING_TIMEOUT_TASK'',''Replication'',0)
	,(''HADR_SEEDING_WAIT_FOR_COMPLETION'',''Replication'',0)
	,(''HADR_SYNC_COMMIT'',''Replication'',0)
	,(''HADR_SYNCHRONIZING_THROTTLE'',''Replication'',0)
	,(''HADR_TDS_LISTENER_SYNC'',''Replication'',0)
	,(''HADR_TDS_LISTENER_SYNC_PROCESSING'',''Replication'',0)
	,(''HADR_THROTTLE_LOG_RATE_GOVERNOR'',''Log Rate Governor'',0)
	,(''HADR_TIMER_TASK'',''Replication'',0)
	,(''HADR_TRANSPORT_DBRLIST'',''Replication'',0)
	,(''HADR_TRANSPORT_FLOW_CONTROL'',''Replication'',0)
	,(''HADR_TRANSPORT_SESSION'',''Replication'',0)
	,(''HADR_WORK_POOL'',''Replication'',0)
	,(''HADR_WORK_QUEUE'',''Replication'',0)
	,(''HADR_XRF_STACK_ACCESS'',''Replication'',0)
	,(''INSTANCE_LOG_RATE_GOVERNOR'',''Log Rate Governor'',0)
	,(''BROKER_TASK_SUBMIT'',''Service Broker'',0)
	,(''BROKER_TO_FLUSH'',''Service Broker'',0)
	,(''BROKER_TRANSMISSION_OBJECT'',''Service Broker'',0)
	,(''BROKER_TRANSMISSION_TABLE'',''Service Broker'',0)
	,(''BROKER_TRANSMISSION_WORK'',''Service Broker'',0)
	,(''BROKER_FORWARDER'',''Service Broker'',0)
	,(''CXCONSUMER'',''Parallelism'',0)
	,(''DTCNEW_ENLIST'',''Transaction'',0)
	,(''DTCNEW_PREPARE'',''Transaction'',0)
	,(''DTCNEW_RECOVERY'',''Transaction'',0)
	,(''DTCNEW_TM'',''Transaction'',0)
	,(''DTCNEW_TRANSACTION_ENLISTMENT'',''Transaction'',0)
	,(''DTCPNTSYNC'',''Transaction'',0)
	,(''BROKER_DISPATCHER'',''Service Broker'',0)
	,(''BROKER_SERVICE'',''Service Broker'',0)
	,(''BROKER_START'',''Service Broker'',0)
	,(''BROKER_TASK_SHUTDOWN'',''Service Broker'',0)
	,(''EXTERNAL_SCRIPT_NETWORK_IOF'',''Network IO'',0)
	,(''FT_COMPROWSET_RWLOCK'',''Full Text Search'',0)
	,(''FT_IFTS_RWLOCK'',''Full Text Search'',0)
	,(''FT_IFTS_SCHEDULER_IDLE_WAIT'',''Idle'',0)
	,(''FT_IFTSHC_MUTEX'',''Full Text Search'',0)
	,(''FT_IFTSISM_MUTEX'',''Full Text Search'',0)
	,(''FT_MASTER_MERGE'',''Full Text Search'',0)
	,(''FT_MASTER_MERGE_COORDINATOR'',''Full Text Search'',0)
	,(''FT_METADATA_MUTEX'',''Full Text Search'',0)
	,(''FT_PROPERTYLIST_CACHE'',''Full Text Search'',0)
	,(''IO_QUEUE_LIMIT'',''Other Disk IO'',0)
	,(''IO_RETRY'',''Other Disk IO'',0)
	,(''POOL_LOG_RATE_GOVERNOR'',''Log Rate Governor'',0)
	,(''PREEMPTIVE_ABR'',''Preemptive'',0)
	,(''PREEMPTIVE_CLOSEBACKUPMEDIA'',''Preemptive'',0)
	,(''PREEMPTIVE_CLOSEBACKUPTAPE'',''Preemptive'',0)
	,(''PREEMPTIVE_CLOSEBACKUPVDIDEVICE'',''Preemptive'',0)
	,(''PREEMPTIVE_CLUSAPI_CLUSTERRESOURCECONTROL'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_COCREATEINSTANCE'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_COGETCLASSOBJECT'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_CREATEACCESSOR'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_DELETEROWS'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_GETCOMMANDTEXT'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_GETDATA'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_GETNEXTROWS'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_GETRESULT'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_GETROWSBYBOOKMARK'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_LBFLUSH'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_LBLOCKREGION'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_LBREADAT'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_LBSETSIZE'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_LBSTAT'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_LBUNLOCKREGION'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_LBWRITEAT'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_QUERYINTERFACE'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_RELEASE'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_RELEASEACCESSOR'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_RELEASEROWS'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_RELEASESESSION'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_RESTARTPOSITION'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_SEQSTRMREAD'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_SEQSTRMREADANDWRITE'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_SETDATAFAILURE'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_SETPARAMETERINFO'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_SETPARAMETERPROPERTIES'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_STRMLOCKREGION'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_STRMSEEKANDREAD'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_STRMSEEKANDWRITE'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_STRMSETSIZE'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_STRMSTAT'',''Preemptive'',0)
	,(''PREEMPTIVE_COM_STRMUNLOCKREGION'',''Preemptive'',0)
	,(''PREEMPTIVE_CONSOLEWRITE'',''Preemptive'',0)
	,(''PREEMPTIVE_CREATEPARAM'',''Preemptive'',0)
	,(''PREEMPTIVE_DEBUG'',''Preemptive'',0)
	,(''PREEMPTIVE_DFSADDLINK'',''Preemptive'',0)
	,(''PREEMPTIVE_DFSLINKEXISTCHECK'',''Preemptive'',0)
	,(''PREEMPTIVE_DFSLINKHEALTHCHECK'',''Preemptive'',0)
	,(''PREEMPTIVE_DFSREMOVELINK'',''Preemptive'',0)
	,(''PREEMPTIVE_DFSREMOVEROOT'',''Preemptive'',0)
	,(''PREEMPTIVE_DFSROOTFOLDERCHECK'',''Preemptive'',0)
	,(''PREEMPTIVE_DFSROOTINIT'',''Preemptive'',0)
	,(''PREEMPTIVE_DFSROOTSHARECHECK'',''Preemptive'',0)
	,(''PREEMPTIVE_DTC_ABORT'',''Preemptive'',0)
	,(''PREEMPTIVE_DTC_ABORTREQUESTDONE'',''Preemptive'',0)
	,(''PREEMPTIVE_DTC_BEGINTRANSACTION'',''Preemptive'',0)
	,(''PREEMPTIVE_DTC_COMMITREQUESTDONE'',''Preemptive'',0)
	,(''PREEMPTIVE_DTC_ENLIST'',''Preemptive'',0)
	,(''PREEMPTIVE_DTC_PREPAREREQUESTDONE'',''Preemptive'',0)
	,(''PREEMPTIVE_FILESIZEGET'',''Preemptive'',0)
	,(''PREEMPTIVE_FSAOLEDB_ABORTTRANSACTION'',''Preemptive'',0)
	,(''PREEMPTIVE_FSAOLEDB_COMMITTRANSACTION'',''Preemptive'',0)
	,(''PREEMPTIVE_FSAOLEDB_STARTTRANSACTION'',''Preemptive'',0)
	,(''PREEMPTIVE_FSRECOVER_UNCONDITIONALUNDO'',''Preemptive'',0)
	,(''PREEMPTIVE_GETRMINFO'',''Preemptive'',0)
	,(''PREEMPTIVE_HADR_LEASE_MECHANISM'',''Preemptive'',0)
	,(''PREEMPTIVE_HTTP_EVENT_WAIT'',''Preemptive'',0)
	,(''PREEMPTIVE_HTTP_REQUEST'',''Preemptive'',0)
	,(''PREEMPTIVE_LOCKMONITOR'',''Preemptive'',0)
	,(''PREEMPTIVE_MSS_RELEASE'',''Preemptive'',0)
	,(''PREEMPTIVE_ODBCOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OLE_UNINIT'',''Preemptive'',0)
	,(''PREEMPTIVE_OLEDB_ABORTORCOMMITTRAN'',''Preemptive'',0)
	,(''PREEMPTIVE_OLEDB_ABORTTRAN'',''Preemptive'',0)
	,(''PREEMPTIVE_OLEDB_GETDATASOURCE'',''Preemptive'',0)
	,(''PREEMPTIVE_OLEDB_GETLITERALINFO'',''Preemptive'',0)
	,(''PREEMPTIVE_OLEDB_GETPROPERTIES'',''Preemptive'',0)
	,(''PREEMPTIVE_OLEDB_GETPROPERTYINFO'',''Preemptive'',0)
	,(''PREEMPTIVE_OLEDB_GETSCHEMALOCK'',''Preemptive'',0)
	,(''PREEMPTIVE_OLEDB_JOINTRANSACTION'',''Preemptive'',0)
	,(''PREEMPTIVE_OLEDB_RELEASE'',''Preemptive'',0)
	,(''PREEMPTIVE_OLEDB_SETPROPERTIES'',''Preemptive'',0)
	,(''PREEMPTIVE_OLEDBOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_ACCEPTSECURITYCONTEXT'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_ACQUIRECREDENTIALSHANDLE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_AUTHENTICATIONOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_AUTHORIZATIONOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_AUTHZGETINFORMATIONFROMCONTEXT'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_AUTHZINITIALIZECONTEXTFROMSID'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_AUTHZINITIALIZERESOURCEMANAGER'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_BACKUPREAD'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_CLOSEHANDLE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_CLUSTEROPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_COMOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_COMPLETEAUTHTOKEN'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_COPYFILE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_CREATEDIRECTORY'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_CREATEFILE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_CRYPTACQUIRECONTEXT'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_CRYPTIMPORTKEY'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_CRYPTOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_DECRYPTMESSAGE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_DELETEFILE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_DELETESECURITYCONTEXT'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_DEVICEIOCONTROL'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_DEVICEOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_DIRSVC_NETWORKOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_DISCONNECTNAMEDPIPE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_DOMAINSERVICESOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_DSGETDCNAME'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_DTCOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_ENCRYPTMESSAGE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_FILEOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_FINDFILE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_FLUSHFILEBUFFERS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_FORMATMESSAGE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_FREECREDENTIALSHANDLE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_FREELIBRARY'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_GENERICOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_GETADDRINFO'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_GETCOMPRESSEDFILESIZE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_GETDISKFREESPACE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_GETFILEATTRIBUTES'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_GETFILESIZE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_GETFINALFILEPATHBYHANDLE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_GETLONGPATHNAME'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_GETPROCADDRESS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_GETVOLUMENAMEFORVOLUMEMOUNTPOINT'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_GETVOLUMEPATHNAME'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_INITIALIZESECURITYCONTEXT'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_LIBRARYOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_LOADLIBRARY'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_LOGONUSER'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_LOOKUPACCOUNTSID'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_MESSAGEQUEUEOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_MOVEFILE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_NETGROUPGETUSERS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_NETLOCALGROUPGETMEMBERS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_NETUSERGETGROUPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_NETUSERGETLOCALGROUPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_NETUSERMODALSGET'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_NETVALIDATEPASSWORDPOLICY'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_NETVALIDATEPASSWORDPOLICYFREE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_OPENDIRECTORY'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_PDH_WMI_INIT'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_PIPEOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_PROCESSOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_QUERYCONTEXTATTRIBUTES'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_QUERYREGISTRY'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_QUERYSECURITYCONTEXTTOKEN'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_REMOVEDIRECTORY'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_REPORTEVENT'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_REVERTTOSELF'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_RSFXDEVICEOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_SECURITYOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_SERVICEOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_SETENDOFFILE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_SETFILEPOINTER'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_SETFILEVALIDDATA'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_SETNAMEDSECURITYINFO'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_SQLCLROPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_SQMLAUNCH'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_VERIFYSIGNATURE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_VERIFYTRUST'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_VSSOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_WAITFORSINGLEOBJECT'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_WINSOCKOPS'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_WRITEFILE'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_WRITEFILEGATHER'',''Preemptive'',0)
	,(''PREEMPTIVE_OS_WSASETLASTERROR'',''Preemptive'',0)
	,(''PREEMPTIVE_REENLIST'',''Preemptive'',0)
	,(''PREEMPTIVE_RESIZELOG'',''Preemptive'',0)
	,(''PREEMPTIVE_ROLLFORWARDREDO'',''Preemptive'',0)
	,(''PREEMPTIVE_ROLLFORWARDUNDO'',''Preemptive'',0)
	,(''PREEMPTIVE_SB_STOPENDPOINT'',''Preemptive'',0)
	,(''PREEMPTIVE_SERVER_STARTUP'',''Preemptive'',0)
	,(''PREEMPTIVE_SETRMINFO'',''Preemptive'',0)
	,(''PREEMPTIVE_SHAREDMEM_GETDATA'',''Preemptive'',0)
	,(''PREEMPTIVE_SNIOPEN'',''Preemptive'',0)
	,(''PREEMPTIVE_SOSHOST'',''Preemptive'',0)
	,(''PREEMPTIVE_SOSTESTING'',''Preemptive'',0)
	,(''PREEMPTIVE_SP_SERVER_DIAGNOSTICS'',''Preemptive'',0)
	,(''PREEMPTIVE_STARTRM'',''Preemptive'',0)
	,(''PREEMPTIVE_STREAMFCB_CHECKPOINT'',''Preemptive'',0)
	,(''PREEMPTIVE_STREAMFCB_RECOVER'',''Preemptive'',0)
	,(''PREEMPTIVE_STRESSDRIVER'',''Preemptive'',0)
	,(''PREEMPTIVE_TESTING'',''Preemptive'',0)
	,(''PREEMPTIVE_TRANSIMPORT'',''Preemptive'',0)
	,(''PREEMPTIVE_UNMARSHALPROPAGATIONTOKEN'',''Preemptive'',0)
	,(''PREEMPTIVE_VSS_CREATESNAPSHOT'',''Preemptive'',0)
	,(''PREEMPTIVE_VSS_CREATEVOLUMESNAPSHOT'',''Preemptive'',0)
	,(''PREEMPTIVE_XE_CALLBACKEXECUTE'',''Preemptive'',0)
	,(''PREEMPTIVE_XE_CX_FILE_OPEN'',''Preemptive'',0)
	,(''PREEMPTIVE_XE_CX_HTTP_CALL'',''Preemptive'',0)
	,(''PREEMPTIVE_XE_DISPATCHER'',''Preemptive'',0)
	,(''PREEMPTIVE_XE_ENGINEINIT'',''Preemptive'',0)
	,(''PREEMPTIVE_XE_GETTARGETSTATE'',''Preemptive'',0)
	,(''PREEMPTIVE_XE_SESSIONCOMMIT'',''Preemptive'',0)
	,(''PREEMPTIVE_XE_TARGETFINALIZE'',''Preemptive'',0)
	,(''PREEMPTIVE_XE_TARGETINIT'',''Preemptive'',0)
	,(''PREEMPTIVE_XE_TIMERRUN'',''Preemptive'',0)
	,(''PREEMPTIVE_XETESTING'',''Preemptive'',0)
	,(''PWAIT_HADR_ACTION_COMPLETED'',''Replication'',0)
	,(''PWAIT_HADR_CHANGE_NOTIFIER_TERMINATION_SYNC'',''Replication'',0)
	,(''PWAIT_HADR_CLUSTER_INTEGRATION'',''Replication'',0)
	,(''PWAIT_HADR_FAILOVER_COMPLETED'',''Replication'',0)
	,(''PWAIT_HADR_JOIN'',''Replication'',0)
	,(''PWAIT_HADR_OFFLINE_COMPLETED'',''Replication'',0)
	,(''PWAIT_HADR_ONLINE_COMPLETED'',''Replication'',0)
	,(''PWAIT_HADR_POST_ONLINE_COMPLETED'',''Replication'',0)
	,(''PWAIT_HADR_SERVER_READY_CONNECTIONS'',''Replication'',0)
	,(''PWAIT_HADR_WORKITEM_COMPLETED'',''Replication'',0)
	,(''PWAIT_HADRSIM'',''Replication'',0)
	,(''PWAIT_RESOURCE_SEMAPHORE_FT_PARALLEL_QUERY_SYNC'',''Full Text Search'',0)
	,(''LCK_M_BU_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_BU_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_IS_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_IS_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_IU_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_IU_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_IX_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_IX_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_RIn_NL_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_RIn_NL_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_RIn_S_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_RIn_S_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_RIn_U_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_RIn_U_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_RIn_X_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_RIn_X_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_RS_S_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_RS_S_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_RS_U_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_RS_U_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_RX_S_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_RX_S_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_RX_U_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_RX_U_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_RX_X_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_RX_X_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_S_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_S_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_SCH_M_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_SCH_M_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_SCH_S_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_SCH_S_LOW_PRIORITY'',''Lock'',0)
	,(''MEMORY_ALLOCATION_EXT'',''Memory'',0)
	,(''MEMORY_GRANT_UPDATE'',''Memory'',0)
	,(''LCK_M_SIU_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_SIU_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_SIX_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_SIX_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_U_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_U_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_UIX_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_UIX_LOW_PRIORITY'',''Lock'',0)
	,(''LCK_M_X_ABORT_BLOCKERS'',''Lock'',0)
	,(''LCK_M_X_LOW_PRIORITY'',''Lock'',0)
	,(''LOGMGR_PMM_LOG'',''Tran Log IO'',0)
	,(''REPL_HISTORYCACHE_ACCESS'',''Replication'',0)
	,(''REPL_TRANFSINFO_ACCESS'',''Replication'',0)
	,(''REPL_TRANHASHTABLE_ACCESS'',''Replication'',0)
	,(''REPL_TRANTEXTINFO_ACCESS'',''Replication'',0)
	,(''RESERVED_MEMORY_ALLOCATION_EXT'',''Memory'',0)
	,(''SLEEP_MASTERDBREADY'',''Idle'',0)
	,(''SLEEP_MASTERMDREADY'',''Idle'',0)
	,(''SLEEP_MASTERUPGRADED'',''Idle'',0)
	,(''SLEEP_MEMORYPOOL_ALLOCATEPAGES'',''Idle'',0)
	,(''SLEEP_RETRY_VIRTUALALLOC'',''Idle'',0)
	,(''SQLTRACE_FILE_BUFFER'',''Tracing'',0)
	,(''SQLTRACE_FILE_READ_IO_COMPLETION'',''Tracing'',0)
	,(''SQLTRACE_FILE_WRITE_IO_COMPLETION'',''Tracing'',0)
	,(''SQLTRACE_INCREMENTAL_FLUSH_SLEEP'',''Idle'',0)
	,(''SQLTRACE_PENDING_BUFFER_WRITERS'',''Tracing'',0)
	,(''TRACE_EVTNOTIF'',''Tracing'',0)
	,(''WRITE_COMPLETION'',''Other Disk IO'',0)
	,(''SLEEP_WORKSPACE_ALLOCATEPAGE'',''Idle'',0)
	,(''SLEEP_BUFFERPOOL_HELPLW'',''Idle'',0)
	,(''ABR'',''Other'',0)
	,(''ASSEMBLY_LOAD'',''SQLCLR'',0)
	,(''ASYNC_DISKPOOL_LOCK'',''Buffer I/O'',0)
	,(''ASYNC_IO_COMPLETION'',''Other Disk IO'',0)
	,(''ASYNC_NETWORK_IO'',''Network IO'',0)
	,(''BACKUP'',''Backup'',0)
	,(''BACKUP_CLIENTLOCK'',''Backup'',0)
	,(''BACKUP_OPERATOR'',''Backup'',0)
	,(''BACKUPBUFFER'',''Backup'',0)
	,(''BACKUPIO'',''Other Disk IO'',0)
	,(''BACKUPTHREAD'',''Backup'',0)
	,(''BAD_PAGE_PROCESS'',''Other'',0)
	,(''BROKER_CONNECTION_RECEIVE_TASK'',''Service Broker'',0)
	,(''BROKER_ENDPOINT_STATE_MUTEX'',''Service Broker'',0)
	,(''BROKER_EVENTHANDLER'',''Service Broker'',1)
	,(''BROKER_INIT'',''Service Broker'',0)
	,(''BROKER_MASTERSTART'',''Service Broker'',0)
	,(''BROKER_RECEIVE_WAITFOR'',''User Wait'',1)
	,(''BROKER_REGISTERALLENDPOINTS'',''Service Broker'',0)
	,(''BROKER_SHUTDOWN'',''Service Broker'',0)
	,(''BROKER_TASK_STOP'',''Service Broker'',0)
	,(''BROKER_TRANSMITTER'',''Service Broker'',1)
	,(''BUILTIN_HASHKEY_MUTEX'',''Other'',0)
	,(''CHECK_PRINT_RECORD'',''Other'',0)
	,(''CHECKPOINT_QUEUE'',''Idle'',1)
	,(''CHKPT'',''Tran Log IO'',1)
	,(''CLR_AUTO_EVENT'',''SQL CLR'',1)
	,(''CLR_CRST'',''SQL CLR'',0)
	,(''CLR_JOIN'',''SQL CLR'',0)
	,(''CLR_MANUAL_EVENT'',''SQL CLR'',1)
	,(''CLR_MEMORY_SPY'',''SQL CLR'',0)
	,(''CLR_MONITOR'',''SQL CLR'',0)
	,(''CLR_RWLOCK_READER'',''SQL CLR'',0)
	,(''CLR_RWLOCK_WRITER'',''SQL CLR'',0)
	,(''CLR_SEMAPHORE'',''SQL CLR'',0)
	,(''CLR_TASK_START'',''SQL CLR'',0)
	,(''CLRHOST_STATE_ACCESS'',''SQL CLR'',0)
	,(''CMEMPARTITIONED'',''Memory'',0)
	,(''CMEMTHREAD'',''Memory'',0)
	,(''CPU'',''CPU'',0)
	,(''CURSOR'',''Other'',0)
	,(''CURSOR_ASYNC'',''Other'',0)
	,(''CXPACKET'',''Parallelism'',1)
	,(''DAC_INIT'',''Other'',0)
	,(''DBCC_COLUMN_TRANSLATION_CACHE'',''Other'',0)
	,(''DBMIRROR_DBM_EVENT'',''Mirroring'',0)
	,(''DBMIRROR_DBM_MUTEX'',''Mirroring'',0)
	,(''DBMIRROR_EVENTS_QUEUE'',''Mirroring'',0)
	,(''DBMIRROR_SEND'',''Mirroring'',0)
	,(''DBMIRROR_WORKER_QUEUE'',''Mirroring'',0)
	,(''DBMIRRORING_CMD'',''Mirroring'',0)
	,(''DBTABLE'',''Other'',0)
	,(''DEADLOCK_ENUM_MUTEX'',''Latch'',0)
	,(''DEADLOCK_TASK_SEARCH'',''Other'',0)
	,(''DEBUG'',''Other'',0)
	,(''DISABLE_VERSIONING'',''Other'',0)
	,(''DISKIO_SUSPEND'',''Backup'',0)
	,(''DLL_LOADING_MUTEX'',''Other'',0)
	,(''DROPTEMP'',''Other'',0)
	,(''DTC'',''Transaction'',0)
	,(''DTC_ABORT_REQUEST'',''Transaction'',0)
	,(''DTC_RESOLVE'',''Transaction'',0)
	,(''DTC_STATE'',''Transaction'',0)
	,(''DTC_TMDOWN_REQUEST'',''Transaction'',0)
	,(''DTC_WAITFOR_OUTCOME'',''Transaction'',0)
	,(''DUMP_LOG_COORDINATOR'',''Other'',0)
	,(''DUMP_LOG_COORDINATOR_QUEUE'',''Other'',0)
	,(''DUMPTRIGGER'',''Other'',0)
	,(''EC'',''Other'',0)
	,(''EE_PMOLOCK'',''Memory'',0)
	,(''EE_SPECPROC_MAP_INIT'',''Other'',0)
	,(''ENABLE_VERSIONING'',''Other'',0)
	,(''ERROR_REPORTING_MANAGER'',''Other'',0)
	,(''EXCHANGE'',''Parallelism'',1)
	,(''EXECSYNC'',''Parallelism'',1)
	,(''EXECUTION_PIPE_EVENT_INTERNAL'',''Other'',0)
	,(''FAILPOINT'',''Other'',0)
	,(''FCB_REPLICA_READ'',''Replication'',0)
	,(''FCB_REPLICA_WRITE'',''Replication'',0)
	,(''FS_GARBAGE_COLLECTOR_SHUTDOWN'',''SQLCLR'',0)
	,(''FSAGENT'',''Idle'',1)
	,(''FT_RESTART_CRAWL'',''Full Text Search'',0)
	,(''FT_RESUME_CRAWL'',''Other'',0)
	,(''FULLTEXT GATHERER'',''Full Text Search'',0)
	,(''GUARDIAN'',''Other'',0)
	,(''HTTP_ENDPOINT_COLLCREATE'',''Other'',0)
	,(''HTTP_ENUMERATION'',''Other'',0)
	,(''HTTP_START'',''Other'',0)
	,(''IMP_IMPORT_MUTEX'',''Other'',0)
	,(''IMPPROV_IOWAIT'',''Other'',0)
	,(''INDEX_USAGE_STATS_MUTEX'',''Latch'',0)
	,(''INTERNAL_TESTING'',''Other'',0)
	,(''IO_AUDIT_MUTEX'',''Other'',0)
	,(''IO_COMPLETION'',''Other Disk IO'',0)
	,(''KSOURCE_WAKEUP'',''Idle'',1)
	,(''KTM_ENLISTMENT'',''Other'',0)
	,(''KTM_RECOVERY_MANAGER'',''Other'',0)
	,(''KTM_RECOVERY_RESOLUTION'',''Other'',0)
	,(''LATCH_DT'',''Latch'',0)
	,(''LATCH_EX'',''Latch'',0)
	,(''LATCH_KP'',''Latch'',0)
	,(''LATCH_NL'',''Latch'',0)
	,(''LATCH_SH'',''Latch'',0)
	,(''LATCH_UP'',''Latch'',0)
	,(''LAZYWRITER_SLEEP'',''Idle'',1)
	,(''LCK_M_BU'',''Lock'',0)
	,(''LCK_M_IS'',''Lock'',0)
	,(''LCK_M_IU'',''Lock'',0)
	,(''LCK_M_IX'',''Lock'',0)
	,(''LCK_M_RIn_NL'',''Lock'',0)
	,(''LCK_M_RIn_S'',''Lock'',0)
	,(''LCK_M_RIn_U'',''Lock'',0)
	,(''LCK_M_RIn_X'',''Lock'',0)
	,(''LCK_M_RS_S'',''Lock'',0)
	,(''LCK_M_RS_U'',''Lock'',0)
	,(''LCK_M_RX_S'',''Lock'',0)
	,(''LCK_M_RX_U'',''Lock'',0)
	,(''LCK_M_RX_X'',''Lock'',0)
	,(''LCK_M_S'',''Lock'',0)
	,(''LCK_M_SCH_M'',''Lock'',0)
	,(''LCK_M_SCH_S'',''Lock'',0)
	,(''LCK_M_SIU'',''Lock'',0)
	,(''LCK_M_SIX'',''Lock'',0)
	,(''LCK_M_U'',''Lock'',0)
	,(''LCK_M_UIX'',''Lock'',0)
	,(''LCK_M_X'',''Lock'',0)
	,(''LOGBUFFER'',''Tran Log IO'',0)
	,(''LOGMGR'',''Tran Log IO'',0)
	,(''LOGMGR_FLUSH'',''Tran Log IO'',0)
	,(''LOGMGR_QUEUE'',''Idle'',1)
	,(''LOGMGR_RESERVE_APPEND'',''Tran Log IO'',0)
	,(''LOWFAIL_MEMMGR_QUEUE'',''Memory'',0)
	,(''MIRROR_SEND_MESSAGE'',''Other'',0)
	,(''MISCELLANEOUS'',''Other'',0)
	,(''MSQL_DQ'',''Network I/O'',0)
	,(''MSQL_SYNC_PIPE'',''Other'',0)
	,(''MSQL_XACT_MGR_MUTEX'',''Transaction'',0)
	,(''MSQL_XACT_MUTEX'',''Transaction'',0)
	,(''MSQL_XP'',''Other'',0)
	,(''MSSEARCH'',''Full Text Search'',0)
	,(''NET_WAITFOR_PACKET'',''Network IO'',0)
	,(''OLEDB'',''Network I/O'',0)
	,(''ONDEMAND_TASK_QUEUE'',''Idle'',1)
	,(''PAGEIOLATCH_DT'',''Buffer IO'',0)
	,(''PAGEIOLATCH_EX'',''Buffer IO'',0)
	,(''PAGEIOLATCH_KP'',''Buffer IO'',0)
	,(''PAGEIOLATCH_NL'',''Buffer IO'',0)
	,(''PAGEIOLATCH_SH'',''Buffer IO'',0)
	,(''PAGEIOLATCH_UP'',''Buffer IO'',0)
	,(''PAGELATCH_DT'',''Buffer Latch'',0)
	,(''PAGELATCH_EX'',''Buffer Latch'',0)
	,(''PAGELATCH_KP'',''Buffer Latch'',0)
	,(''PAGELATCH_NL'',''Buffer Latch'',0)
	,(''PAGELATCH_SH'',''Buffer Latch'',0)
	,(''PAGELATCH_UP'',''Buffer Latch'',0)
	,(''PARALLEL_BACKUP_QUEUE'',''Other'',0)
	,(''PRINT_ROLLBACK_PROGRESS'',''Other'',0)
	,(''QNMANAGER_ACQUIRE'',''Other'',0)
	,(''QPJOB_KILL'',''Other'',0)
	,(''QPJOB_WAITFOR_ABORT'',''Other'',0)
	,(''QRY_MEM_GRANT_INFO_MUTEX'',''Other'',0)
	,(''QUERY_ERRHDL_SERVICE_DONE'',''Other'',0)
	,(''QUERY_EXECUTION_INDEX_SORT_EVENT_OPEN'',''Other'',0)
	,(''QUERY_NOTIFICATION_MGR_MUTEX'',''Other'',0)
	,(''QUERY_NOTIFICATION_SUBSCRIPTION_MUTEX'',''Other'',0)
	,(''QUERY_NOTIFICATION_TABLE_MGR_MUTEX'',''Other'',0)
	,(''QUERY_NOTIFICATION_UNITTEST_MUTEX'',''Other'',0)
	,(''QUERY_OPTIMIZER_PRINT_MUTEX'',''Other'',0)
	,(''QUERY_REMOTE_BRICKS_DONE'',''Other'',0)
	,(''QUERY_TRACEOUT'',''Tracing'',0)
	,(''RECOVER_CHANGEDB'',''Other'',0)
	,(''REPL_CACHE_ACCESS'',''Replication'',0)
	,(''REPL_SCHEMA_ACCESS'',''Replication'',0)
	,(''REPLICA_WRITES'',''Replication'',0)
	,(''REQUEST_DISPENSER_PAUSE'',''Other'',0)
	,(''REQUEST_FOR_DEADLOCK_SEARCH'',''Idle'',1)
	,(''RESOURCE_QUEUE'',''Idle'',1)
	,(''RESOURCE_SEMAPHORE'',''Memory'',0)
	,(''RESOURCE_SEMAPHORE_MUTEX'',''Compilation'',0)
	,(''RESOURCE_SEMAPHORE_QUERY_COMPILE'',''Compilation'',0)
	,(''RESOURCE_SEMAPHORE_SMALL_QUERY'',''Compilation'',0)
	,(''SEC_DROP_TEMP_KEY'',''Other'',0)
	,(''SEQUENTIAL_GUID'',''Other'',0)
	,(''SERVER_IDLE_CHECK'',''Idle'',1)
	,(''SHUTDOWN'',''Other'',0)
	,(''SLEEP_BPOOL_FLUSH'',''Idle'',1)
	,(''SLEEP_DBSTARTUP'',''Idle'',1)
	,(''SLEEP_DCOMSTARTUP'',''Idle'',1)
	,(''SLEEP_MSDBSTARTUP'',''Idle'',1)
	,(''SLEEP_SYSTEMTASK'',''Idle'',1)
	,(''SLEEP_TASK'',''Idle'',1)
	,(''SLEEP_TEMPDBSTARTUP'',''Idle'',1)
	,(''SNI_CRITICAL_SECTION'',''Other'',0)
	,(''SNI_HTTP_ACCEPT'',''Idle'',1)
	,(''SNI_HTTP_WAITFOR_0_DISCON'',''Other'',0)
	,(''SNI_LISTENER_ACCESS'',''Other'',0)
	,(''SNI_TASK_COMPLETION'',''Other'',0)
	,(''SOAP_READ'',''Full Text Search'',0)
	,(''SOAP_WRITE'',''Full Text Search'',0)
	,(''SOS_CALLBACK_REMOVAL'',''Other'',0)
	,(''SOS_DISPATCHER_MUTEX'',''Other'',0)
	,(''SOS_LOCALALLOCATORLIST'',''Other'',0)
	,(''SOS_OBJECT_STORE_DESTROY_MUTEX'',''Other'',0)
	,(''SOS_PROCESS_AFFINITY_MUTEX'',''Other'',0)
	,(''SOS_RESERVEDMEMBLOCKLIST'',''Memory'',0)
	,(''SOS_SCHEDULER_YIELD'',''CPU'',0)
	,(''SOS_STACKSTORE_INIT_MUTEX'',''Other'',0)
	,(''SOS_SYNC_TASK_ENQUEUE_EVENT'',''Other'',0)
	,(''SOS_VIRTUALMEMORY_LOW'',''Memory'',0)
	,(''SOSHOST_EVENT'',''Other'',0)
	,(''SOSHOST_INTERNAL'',''Other'',0)
	,(''SOSHOST_MUTEX'',''Other'',0)
	,(''SOSHOST_RWLOCK'',''Other'',0)
	,(''SOSHOST_SEMAPHORE'',''Other'',0)
	,(''SOSHOST_SLEEP'',''Other'',0)
	,(''SOSHOST_TRACELOCK'',''Other'',0)
	,(''SOSHOST_WAITFORDONE'',''Other'',0)
	,(''SQLCLR_APPDOMAIN'',''SQL CLR'',0)
	,(''SQLCLR_ASSEMBLY'',''SQL CLR'',0)
	,(''SQLCLR_DEADLOCK_DETECTION'',''SQL CLR'',0)
	,(''SQLCLR_QUANTUM_PUNISHMENT'',''SQL CLR'',0)
	,(''SQLSORT_NORMMUTEX'',''Other'',0)
	,(''SQLSORT_SORTMUTEX'',''Other'',0)
	,(''SQLTRACE_BUFFER_FLUSH'',''Idle'',1)
	,(''SQLTRACE_LOCK'',''Other'',0)
	,(''SQLTRACE_SHUTDOWN'',''Tracing'',0)
	,(''SQLTRACE_WAIT_ENTRIES'',''Idle'',0)
	,(''SRVPROC_SHUTDOWN'',''Other'',0)
	,(''TEMPOBJ'',''Other'',0)
	,(''THREADPOOL'',''Worker Thread'',0)
	,(''TIMEPRIV_TIMEPERIOD'',''Other'',0)
	,(''TRACEWRITE'',''Tracing'',1)
	,(''TRAN_MARKLATCH_DT'',''Transaction'',0)
	,(''TRAN_MARKLATCH_EX'',''Transaction'',0)
	,(''TRAN_MARKLATCH_KP'',''Transaction'',0)
	,(''TRAN_MARKLATCH_NL'',''Transaction'',0)
	,(''TRAN_MARKLATCH_SH'',''Transaction'',0)
	,(''TRAN_MARKLATCH_UP'',''Transaction'',0)
	,(''TRANSACTION_MUTEX'',''Transaction'',0)
	,(''UTIL_PAGE_ALLOC'',''Memory'',0)
	,(''VIA_ACCEPT'',''Other'',0)
	,(''VIEW_DEFINITION_MUTEX'',''Latch'',0)
	,(''WAIT_FOR_RESULTS'',''User Wait'',1)
	,(''WAITFOR'',''User Wait'',1)
	,(''WAITFOR_TASKSHUTDOWN'',''Idle'',1)
	,(''WAITSTAT_MUTEX'',''Other'',0)
	,(''WCC'',''Other'',0)
	,(''WORKTBL_DROP'',''Other'',0)
	,(''WRITELOG'',''Tran Log IO'',0)
	,(''XACT_OWN_TRANSACTION'',''Transaction'',0)
	,(''XACT_RECLAIM_SESSION'',''Transaction'',0)
	,(''XACTLOCKINFO'',''Transaction'',0)
	,(''XACTWORKSPACE_MUTEX'',''Transaction'',0)
	,(''XE_BUFFERMGR_ALLPROCECESSED_EVENT'',''Other'',0)
	,(''XE_BUFFERMGR_FREEBUF_EVENT'',''Other'',0)
	,(''XE_DISPATCHER_JOIN'',''Other'',0)
	,(''XE_DISPATCHER_WAIT'',''Idle'',1)
	,(''XE_MODULEMGR_SYNC'',''Other'',0)
	,(''XE_OLS_LOCK'',''Other'',0)
	,(''XE_SERVICES_MUTEX'',''Other'',0)
	,(''XE_SESSION_CREATE_SYNC'',''Other'',0)
	,(''XE_SESSION_SYNC'',''Other'',0)
	,(''XE_STM_CREATE'',''Other'',0)
	,(''XE_TIMER_EVENT'',''Idle'',1)
	,(''XE_TIMER_MUTEX'',''Other'',0)
) AS WsCat ([Wait Type],[Wait Category],[Ignore])
	ON bWS.[wait_type] = WsCat.[Wait Type]
'

IF @baselineSchema IS NOT NULL
	BEGIN
		SET @sql = REPLACE(@PowerBI_WaitStats, '{0}', @baselineSchema);
		EXEC(@sql);
	END
IF @replaySchema IS NOT NULL
	BEGIN
		SET @sql = REPLACE(@PowerBI_WaitStats, '{0}', @replaySchema);
		EXEC(@sql);
	END

--===========================================================
DECLARE @PowerBI_Time nvarchar(max) = N'
CREATE VIEW {0}.[PowerBI_Time] AS
SELECT
	SUM([duration_minutes]) OVER(ORDER BY [end_time] ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS [Elapsed Time (min)]
FROM {0}.[Intervals]
'

IF @baselineSchema IS NOT NULL
	BEGIN
		SET @sql = REPLACE(@PowerBI_Time, '{0}', @baselineSchema);
		EXEC(@sql);
	END
IF @replaySchema IS NOT NULL
	BEGIN
		SET @sql = REPLACE(@PowerBI_Time, '{0}', @replaySchema);
		EXEC(@sql);
	END

END