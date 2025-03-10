﻿using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using WorkloadTools.Util;

namespace WorkloadTools
{
    public abstract class WorkloadListener : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public SqlConnectionInfo ConnectionInfo { get; set; }
        public string Source { get; set; }

        private string[] _applicationFilter;
        private string[] _databaseFilter;
        private string[] _hostFilter;
        private string[] _loginFilter;

        public string[] ApplicationFilter
        {
            get => _applicationFilter;
            set
            {
                _applicationFilter = value;
                if (_filter != null)
                {
                    _filter.ApplicationFilter.PredicateValue = _applicationFilter;
                }
            }
        }
        public string[] DatabaseFilter
        {
            get => _databaseFilter;
            set
            {
                _databaseFilter = value;
                if (_filter != null)
                {
                    _filter.DatabaseFilter.PredicateValue = _databaseFilter;
                }
            }
        }
        public string[] HostFilter
        {
            get => _hostFilter;
            set
            {
                _hostFilter = value;
                if (_filter != null)
                {
                    _filter.HostFilter.PredicateValue = _hostFilter;
                }
            }
        }
        public string[] LoginFilter
        {
            get => _loginFilter;
            set
            {
                _loginFilter = value;
                if (_filter != null)
                {
                    _filter.LoginFilter.PredicateValue = _loginFilter;
                }
            }
        }

        public int StatsCollectionIntervalSeconds { get; set; } = 60;
        public int TimeoutMinutes { get; set; } = 0;
        public DateTime StartAt { get; set; } = DateTime.Now;

        private WorkloadEventFilter _filter;

        protected WorkloadEventFilter Filter
        {
            get
            {
                if (_filter != null)
                {
                    return _filter;
                }
                else
                {
                    return null;
                }
            }
            set => _filter = value;
        }

        protected IEventQueue Events;

        public EventQueueType QueueType = EventQueueType.BinarySerialized;

        protected bool stopped;

        public WorkloadListener()
        {
            switch (QueueType)
            {
                case EventQueueType.MMF:
                    Events = new MMFEventQueue();
                    break;
                case EventQueueType.LiteDB:
                    throw new NotImplementedException();
                case EventQueueType.Sqlite:
                    throw new NotImplementedException();
                case EventQueueType.BinarySerialized:
                    Events = new BinarySerializedBufferedEventQueue();
                    Events.BufferSize = 10000;
                    break;
            }
            
        }

        public void Dispose() {
            stopped = true;
            Events.Dispose();
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected abstract void Dispose(bool disposing);

        public abstract WorkloadEvent Read();

        public abstract void Initialize();

        public bool IsRunning { get { return !stopped; } }

        // Collects some performance counters
        protected virtual void ReadPerfCountersEvents()
        {
            try
            {
                while (!stopped)
                {
                    var evt = new CounterWorkloadEvent();
                    evt.Type = WorkloadEvent.EventType.PerformanceCounter;
                    evt.StartTime = DateTime.Now;

                    evt.Counters.Add(
                        CounterWorkloadEvent.CounterNameEnum.AVG_CPU_USAGE,
                        GetLastCPUUsage()
                    );

                    Events.Enqueue(evt);

                    Thread.Sleep(StatsCollectionIntervalSeconds * 1000); // 1 minute
                }
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);
                logger.Error(ex.StackTrace);

                if (ex.InnerException != null)
                {
                    logger.Error(ex.InnerException.Message);
                }
            }
        }

        private int GetLastCPUUsage()
        {

            using (var conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString();
                conn.Open();
                // Calculate CPU usage during the last minute interval
                var sql = @"
                    IF SERVERPROPERTY('Edition') = 'SQL Azure'
                        AND SERVERPROPERTY('EngineEdition') = 5
                    BEGIN
                        WITH CPU_Usage AS (
                            SELECT avg_cpu_percent, end_time AS Event_Time
                            FROM sys.dm_db_resource_stats WITH (NOLOCK) 
                        )
                        SELECT 
                            CAST(ISNULL(AVG(avg_cpu_percent),0) AS int) AS avg_CPU_percent
                        FROM CPU_Usage
                        WHERE [Event_Time] >= DATEADD(minute, -{0}, GETDATE())
                        OPTION (RECOMPILE);
                    END

                    IF SERVERPROPERTY('Edition') = 'SQL Azure'
                        AND SERVERPROPERTY('EngineEdition') = 8 -- Managed Instance
                    BEGIN
                        WITH PerfCounters AS (
	                        SELECT DISTINCT
	                             RTrim(spi.[object_name]) AS [object_name]
	                            ,RTrim(spi.[counter_name]) AS [counter_name]
	                            ,RTRIM(spi.instance_name) AS [instance_name]
	                            ,CAST(spi.[cntr_value] AS BIGINT) AS [cntr_value]
	                            ,spi.[cntr_type]
	                        FROM sys.dm_os_performance_counters AS spi 
	                        LEFT JOIN sys.databases AS d
		                        ON LEFT(spi.[instance_name], 36) -- some instance_name values have an additional identifier appended after the GUID
		                        = d.[name]
	                        WHERE
		                        counter_name IN (
			                         'CPU usage %'
			                        ,'CPU usage % base'
		                        ) 
                        )
                        SELECT CAST(SUM(value) AS int) AS avg_CPU_percent
                        FROM (
                            SELECT 
	                            CAST(CASE WHEN pc.[cntr_type] = 537003264 AND pc1.[cntr_value] > 0 THEN (pc.[cntr_value] * 1.0) / (pc1.[cntr_value] * 1.0) * 100 ELSE pc.[cntr_value] END AS float(10)) AS [value]
                            from PerfCounters pc
                            LEFT OUTER JOIN PerfCounters AS pc1
	                            ON (
		                            pc.[counter_name] = REPLACE(pc1.[counter_name],' base','')
		                            OR pc.[counter_name] = REPLACE(pc1.[counter_name],' base',' (ms)')
	                            )
	                            AND pc.[object_name] = pc1.[object_name]
	                            AND pc.[instance_name] = pc1.[instance_name]
	                            AND pc1.[counter_name] LIKE '%base'
                            WHERE
	                            pc.[counter_name] NOT LIKE '% base'
                                AND pc.object_name LIKE '%:Resource Pool Stats'
                        ) AS p
                        OPTION (RECOMPILE);
                    END


                    ELSE -- On Premises

                    BEGIN
                        WITH ts_now(ts_now) AS (
                            SELECT cpu_ticks/(cpu_ticks/ms_ticks) FROM sys.dm_os_sys_info WITH (NOLOCK)
                        ),
                        CPU_Usage AS (
                            SELECT TOP(256) SQLProcessUtilization, 
                                            DATEADD(ms, -1 * (ts_now.ts_now - [timestamp]), GETDATE()) AS [Event_Time] 
                            FROM (
                                SELECT record.value('(./Record/@id)[1]', 'int') AS record_id, 
                                    record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') 
                                    AS [SystemIdle], 
                                    record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') 
                                    AS [SQLProcessUtilization], [timestamp] 
                                FROM (
                                    SELECT [timestamp], CONVERT(xml, record) AS [record] 
                                    FROM sys.dm_os_ring_buffers WITH (NOLOCK)
                                    WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' 
                                        AND record LIKE N'%<SystemHealth>%'
                                ) AS x
                            ) AS y 
                            CROSS JOIN ts_now
                        )
                        SELECT 
                            ISNULL(AVG(SQLProcessUtilization),0) AS avg_CPU_percent
                        FROM CPU_Usage
                        WHERE [Event_Time] >= DATEADD(minute, -{0}, GETDATE())
                        OPTION (RECOMPILE);
                    END
                ";

                sql = string.Format(sql,StatsCollectionIntervalSeconds / 60);

                var avg_CPU_percent = -1;

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = sql;
                    avg_CPU_percent = (int)cmd.ExecuteScalar();
                }

                return avg_CPU_percent;
            }
        }

        protected virtual void ReadWaitStatsEvents()
        {
            try
            {
                DataTable lastWaits = null;
                while (!stopped)
                {
                    var evt = new WaitStatsWorkloadEvent();
                    evt.Type = WorkloadEvent.EventType.WAIT_stats;
                    evt.StartTime = DateTime.Now;

                    var newWaits = GetWaits();
                    evt.Waits = GetDiffWaits(newWaits, lastWaits);
                    lastWaits = newWaits;

                    Events.Enqueue(evt);

                    Thread.Sleep(StatsCollectionIntervalSeconds * 1000); // 1 minute
                }
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);
                logger.Error(ex.StackTrace);

                if (ex.InnerException != null)
                {
                    logger.Error(ex.InnerException.Message);
                }
            }
        }

        private DataTable GetDiffWaits(DataTable newWaits, DataTable lastWaits)
        {
            // no baseline established already
            // return all zeros
            if (lastWaits == null)
            {
                var result = newWaits.Clone();
                foreach (DataRow dr in newWaits.Rows)
                {
                    var nr = result.Rows.Add();
                    nr["wait_type"] = dr["wait_type"];
                    nr["wait_sec"] = 0;
                    nr["resource_sec"] = 0;
                    nr["signal_sec"] = 0;
                    nr["wait_count"] = 0;
                }
                return result;
            }

            // catch the case when stats are reset
            long newWaitCount = 0;
            var newWaitCountObj = newWaits.Compute("SUM(wait_count)", null);
            if (newWaitCountObj != DBNull.Value)
            {
                newWaitCount = Convert.ToInt64(newWaitCountObj);
            }
            long lastWaitCount = 0;
            var lastWaitCountObj = lastWaits.Compute("SUM(wait_count)", null);
            if (lastWaitCountObj != DBNull.Value)
            {
                lastWaitCount = Convert.ToInt64(lastWaitCountObj);
            }

            // if newWaits < lastWaits --> reset
            // I can return newWaits without having to compute the diff
            if (newWaitCount < lastWaitCount)
            {
                return newWaits;
            }

            var results = from table1 in newWaits.AsEnumerable()
                          join table2 in lastWaits.AsEnumerable()
                                on table1["wait_type"] equals table2["wait_type"]
                          select new
                          {
                              wait_type = Convert.ToString(table1["wait_type"]),
                              wait_sec = Convert.ToDouble(table1["wait_sec"]) - Convert.ToDouble(table2["wait_sec"]),
                              resource_sec = Convert.ToDouble(table1["resource_sec"]) - Convert.ToDouble(table2["resource_sec"]),
                              signal_sec = Convert.ToDouble(table1["signal_sec"]) - Convert.ToDouble(table2["signal_sec"]),
                              wait_count = Convert.ToDouble(table1["wait_count"]) - Convert.ToDouble(table2["wait_count"])
                          };

            return DataUtils.ToDataTable(results.Where(w => w.wait_sec > 0));
        }

        private DataTable GetWaits()
        {

            using (var conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString();
                conn.Open();
                // Calculate waits since instance restart
                var sql = @"
                    WITH [Waits] 
                    AS (
	                    SELECT wait_type, wait_time_ms/ 1000.0 AS [WaitS],
                              (wait_time_ms - signal_wait_time_ms) / 1000.0 AS [ResourceS],
                               signal_wait_time_ms / 1000.0 AS [SignalS],
                               waiting_tasks_count AS [WaitCount]
                        FROM sys.dm_os_wait_stats WITH (NOLOCK)
                        WHERE [wait_type] NOT IN (
                            N'BROKER_EVENTHANDLER', N'BROKER_RECEIVE_WAITFOR', N'BROKER_TASK_STOP',
		                    N'BROKER_TO_FLUSH', N'BROKER_TRANSMITTER', N'CHECKPOINT_QUEUE',
                            N'CHKPT', N'CLR_AUTO_EVENT', N'CLR_MANUAL_EVENT', N'CLR_SEMAPHORE',
                            N'DBMIRROR_DBM_EVENT', N'DBMIRROR_EVENTS_QUEUE', N'DBMIRROR_WORKER_QUEUE',
		                    N'DBMIRRORING_CMD', N'DIRTY_PAGE_POLL', N'DISPATCHER_QUEUE_SEMAPHORE',
                            N'EXECSYNC', N'FSAGENT', N'FT_IFTS_SCHEDULER_IDLE_WAIT', N'FT_IFTSHC_MUTEX',
                            N'HADR_CLUSAPI_CALL', N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', N'HADR_LOGCAPTURE_WAIT', 
		                    N'HADR_NOTIFICATION_DEQUEUE', N'HADR_TIMER_TASK', N'HADR_WORK_QUEUE',
                            N'KSOURCE_WAKEUP', N'LAZYWRITER_SLEEP', N'LOGMGR_QUEUE', 
		                    N'MEMORY_ALLOCATION_EXT', N'ONDEMAND_TASK_QUEUE',
		                    N'PARALLEL_REDO_DRAIN_WORKER', N'PARALLEL_REDO_LOG_CACHE', N'PARALLEL_REDO_TRAN_LIST',
		                    N'PARALLEL_REDO_WORKER_SYNC', N'PARALLEL_REDO_WORKER_WAIT_WORK',
		                    N'PREEMPTIVE_HADR_LEASE_MECHANISM', N'PREEMPTIVE_SP_SERVER_DIAGNOSTICS',
		                    N'PREEMPTIVE_OS_LIBRARYOPS', N'PREEMPTIVE_OS_COMOPS', N'PREEMPTIVE_OS_CRYPTOPS',
		                    N'PREEMPTIVE_OS_PIPEOPS', N'PREEMPTIVE_OS_AUTHENTICATIONOPS',
		                    N'PREEMPTIVE_OS_GENERICOPS', N'PREEMPTIVE_OS_VERIFYTRUST',
		                    N'PREEMPTIVE_OS_FILEOPS', N'PREEMPTIVE_OS_DEVICEOPS', N'PREEMPTIVE_OS_QUERYREGISTRY',
		                    N'PREEMPTIVE_OS_WRITEFILE',
		                    N'PREEMPTIVE_XE_CALLBACKEXECUTE', N'PREEMPTIVE_XE_DISPATCHER',
		                    N'PREEMPTIVE_XE_GETTARGETSTATE', N'PREEMPTIVE_XE_SESSIONCOMMIT',
		                    N'PREEMPTIVE_XE_TARGETINIT', N'PREEMPTIVE_XE_TARGETFINALIZE',
                            N'PWAIT_ALL_COMPONENTS_INITIALIZED', N'PWAIT_DIRECTLOGCONSUMER_GETNEXT',
		                    N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
		                    N'QDS_ASYNC_QUEUE',
                            N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', N'REQUEST_FOR_DEADLOCK_SEARCH',
		                    N'RESOURCE_QUEUE', N'SERVER_IDLE_CHECK', N'SLEEP_BPOOL_FLUSH', N'SLEEP_DBSTARTUP',
		                    N'SLEEP_DCOMSTARTUP', N'SLEEP_MASTERDBREADY', N'SLEEP_MASTERMDREADY',
                            N'SLEEP_MASTERUPGRADED', N'SLEEP_MSDBSTARTUP', N'SLEEP_SYSTEMTASK', N'SLEEP_TASK',
                            N'SLEEP_TEMPDBSTARTUP', N'SNI_HTTP_ACCEPT', N'SP_SERVER_DIAGNOSTICS_SLEEP',
		                    N'SQLTRACE_BUFFER_FLUSH', N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', N'SQLTRACE_WAIT_ENTRIES',
		                    N'WAIT_FOR_RESULTS', N'WAITFOR', N'WAITFOR_TASKSHUTDOWN', N'WAIT_XTP_HOST_WAIT',
		                    N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', N'WAIT_XTP_CKPT_CLOSE', N'WAIT_XTP_RECOVERY',
		                    N'XE_BUFFERMGR_ALLPROCESSED_EVENT', N'XE_DISPATCHER_JOIN',
                            N'XE_DISPATCHER_WAIT', N'XE_LIVE_TARGET_TVF', N'XE_TIMER_EVENT')
                        AND waiting_tasks_count > 0
                    )
                    SELECT
	                    W1.wait_type,
                        CAST (MAX (W1.WaitS) AS DECIMAL (16,2)) AS [wait_sec],
                        CAST (MAX (W1.ResourceS) AS DECIMAL (16,2)) AS [resource_sec],
                        CAST (MAX (W1.SignalS) AS DECIMAL (16,2)) AS [signal_sec],
                        MAX (W1.WaitCount) AS [wait_count]
                    FROM Waits AS W1
                    GROUP BY W1.wait_type
                    HAVING CAST (MAX (W1.WaitS) AS DECIMAL (16,2)) > 0
                    ORDER BY wait_sec DESC
                    OPTION (RECOMPILE);
                ";

                DataTable waits = null;

                using (var adapter = new SqlDataAdapter(sql, conn))
                {
                    using (var ds = new DataSet())
                    {
                        _ = adapter.Fill(ds);
                        waits = ds.Tables[0];
                    }
                }

                var results = from table1 in waits.AsEnumerable()
                              select new
                              {
                                  wait_type = Convert.ToString(table1["wait_type"]),
                                  wait_sec = Convert.ToDouble(table1["wait_sec"]),
                                  resource_sec = Convert.ToDouble(table1["resource_sec"]),
                                  signal_sec = Convert.ToDouble(table1["signal_sec"]),
                                  wait_count = Convert.ToDouble(table1["wait_count"])
                              };

                return DataUtils.ToDataTable(results);
            }
        }

        protected virtual void SetTransactionMark(bool allDatabases)
        {

            using (var conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString();
                conn.Open();
                // Create Marked Transaction
                var sql = @"
DECLARE @dbname sysname
DECLARE @sql nvarchar(max), @qry nvarchar(max)

SET @qry = '
PRINT DB_NAME()
BEGIN TRAN WorkloadTools WITH MARK ''WorkloadTools'';
BEGIN TRY
	CREATE TYPE WorkloadToolsType FROM int;
	DROP TYPE WorkloadToolsType;
	IF XACT_STATE() = 1 
		COMMIT TRAN WorkloadTools;
END TRY
BEGIN CATCH
	IF XACT_STATE() <> 0 
		ROLLBACK TRAN WorkloadTools;
END CATCH
'


DECLARE c CURSOR STATIC LOCAL FORWARD_ONLY READ_ONLY FOR
SELECT name
FROM sys.databases 
WHERE database_id > 4
" + (allDatabases ? "" : "AND database_id = DB_ID()") + @"
ORDER BY name

OPEN c 
FETCH NEXT FROM c INTO @dbname

WHILE @@FETCH_STATUS = 0
BEGIN

	SET @sql = 'EXEC ' + QUOTENAME(@dbname) + '.sys.sp_executesql @qry'

	BEGIN TRY
		EXEC sp_executesql @sql, N'@qry nvarchar(max)', @qry
	END TRY
	BEGIN CATCH
		PRINT 'Unable to mark the transaction on database ' + @dbname
	END CATCH

	FETCH NEXT FROM c INTO @dbname
END

CLOSE c
DEALLOCATE c
                ";

                using (var cmd = new SqlCommand(sql, conn))
                {
                    _ = cmd.ExecuteNonQuery();
                }

            }
        }
    }
}
