using NLog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WorkloadTools.Util;
using System.Collections.Concurrent;
using FastMember;
using Microsoft.SqlServer.Management.SqlParser.SqlCodeDom;

namespace WorkloadTools.Consumer.Analysis
{
    public class WorkloadAnalyzer : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public SqlConnectionInfo ConnectionInfo { get; set; }
        public int Interval { get; set; }

        private readonly Dictionary<long, NormalizedQuery> normalizedQueries = new Dictionary<long, NormalizedQuery>();
        private readonly Dictionary<string, int> applications = new Dictionary<string, int>();
        private readonly Dictionary<string, int> databases = new Dictionary<string, int>();
        private readonly Dictionary<string, int> logins = new Dictionary<string, int>();
        private readonly Dictionary<string, int> hosts = new Dictionary<string, int>();

        private Queue<WorkloadEvent> _internalQueue = new Queue<WorkloadEvent>();
        private readonly object _internalQueueLock = new object();
        private Thread Worker;
        private bool stopped = false;
        public int MaxInternalQueueSize { get; set; } = 10000;

        private ConcurrentDictionary<ExecutionDetailKey, AggregatedExecutionStats> rawData;
		private DataTable errorData;
        private readonly SqlTextNormalizer normalizer;
        private bool TargetTableCreated = false;
        private bool FirstIntervalWritten = false;

        private DataTable counterData;
        private DataTable waitsData;
        private DataTable diskPerfData;

        public int MaximumWriteRetries { get; set; }
		public bool TruncateTo4000 { get; set; }
		public bool TruncateTo1024 { get; set; }
        public bool WriteDetail { get; set; } = true;
        public bool WriteSummary { get; set; } = true;

        private class NormalizedQuery
        {
            public long Hash { get; set; }
            public string NormalizedText { get; set; }
            public string ExampleText { get; set; }
        }

        private DateTime lastDump = DateTime.MinValue;
        private DateTime lastEventTime = DateTime.MinValue;

        public WorkloadAnalyzer()
		{
			normalizer = new SqlTextNormalizer()
			{
				TruncateTo1024 = TruncateTo1024,
				TruncateTo4000 = TruncateTo4000
			};
		}

        public bool HasEventsQueued
        {
            get
            {
                lock (_internalQueueLock)
                {
                    return _internalQueue.Count > 0;
                }
            }
        }

        private void CloseInterval()
        {
            // Write collected data to the destination database
            var duration = lastEventTime - lastDump;
            if (duration.TotalMinutes >= Interval)
            {
                try
                {
                    var numRetries = 0;
                    while (numRetries <= MaximumWriteRetries)
                    {
                        try
                        {
                            WriteToServer(lastEventTime);
                            numRetries = MaximumWriteRetries + 1;
                        }
                        catch (Exception ex)
                        {
                            logger.Warn("Unable to write workload analysis.");
                            logger.Warn(ex.Message);

                            if (numRetries == MaximumWriteRetries)
                            {
                                throw;
                            }
                        }
                        numRetries++;
                    }
                }
                catch (Exception e)
                {
                    try
                    {
                        logger.Error(e, "Unable to write workload analysis info to the destination database.");
                        logger.Error(e.StackTrace);
                    }
                    catch
                    {
                        Console.WriteLine(string.Format("Unable to write to the database: {0}.", e.Message));
                    }
                }
                finally
                {
                    lastDump = lastEventTime;
                }
            }

        }

        private void ProcessQueue()
        {
            while (!stopped)
            {
                WorkloadEvent data = null;
                bool hasData = false;

                lock (_internalQueueLock)
                {
                    CloseInterval();

                    if (_internalQueue.Count > 0)
                    {
                        data = _internalQueue.Dequeue();
                        hasData = true;
                        // Notify Add() that a slot is now free
                        Monitor.PulseAll(_internalQueueLock);
                    }
                }

                if (hasData)
                {
                    InternalAdd(data);
                }
                else
                {
                    // Sleep outside the lock so Add() is not blocked unnecessarily
                    Thread.Sleep(10);
                }
            }
        }

        public void Add(WorkloadEvent evt)
        {
            if (evt is ExecutionWorkloadEvent executionEvent && string.IsNullOrEmpty(executionEvent.Text))
            {
                return;
            }

            try
            {
                ProvisionWorker();
            }
            catch (Exception e)
            {
                logger.Error(e, "Unable to start the worker thread for WorkloadAnalyzer");
            }

            lock (_internalQueueLock)
            {
                // Block when the queue is full to avoid unbounded memory growth
                while (!stopped && _internalQueue.Count >= MaxInternalQueueSize)
                {
                    Monitor.Wait(_internalQueueLock);
                }

                if (stopped) return;

                lastEventTime = evt.StartTime;
                if (lastDump == DateTime.MinValue)
                {
                    lastDump = lastEventTime;
                }
                _internalQueue.Enqueue(evt);
            }

        }

        private void ProvisionWorker()
        {
            var startNewWorker = false;
            if (Worker == null)
            {
                startNewWorker = true;
            }
            else
            {
                if (!Worker.IsAlive)
                {
                    startNewWorker = true;
                }
            }

            if (startNewWorker)
            {
                // Start a new background worker if the thread is null
                // or stopped / aborted
                Worker = new Thread(() =>
                {
                    try
                    {
                        ProcessQueue();
                    }
                    catch (Exception e)
                    {
                        logger.Error(e.Message);
                        logger.Error(e.StackTrace);
                    }
                })
                {
                    IsBackground = true,
                    Name = "RealtimeWorkloadAnalyzer.Worker"
                };
                Worker.Start();

                Thread.Sleep(100);
            }
        }

        private void InternalAdd(WorkloadEvent evt)
        {
            if (evt is ExecutionWorkloadEvent executionEvent)
            {
                InternalAdd(executionEvent);
            }

            if (evt is ErrorWorkloadEvent errorEvent)
            {
                InternalAdd(errorEvent);
            }

            if (evt is CounterWorkloadEvent counterEvent)
            {
                InternalAdd(counterEvent);
            }

            if (evt is WaitStatsWorkloadEvent waitStatsEvent)
            {
                InternalAdd(waitStatsEvent);
            }

            if (evt is DiskPerfWorkloadEvent diskPerfEvent)
            {
                InternalAdd(diskPerfEvent);
            }
        }

		private void InternalAdd(ErrorWorkloadEvent evt)
		{
			var row = errorData.NewRow();
			row.SetField("message", evt.Text);
            row.SetField("type", evt.Type);
            errorData.Rows.Add(row);
		}

		private void InternalAdd(WaitStatsWorkloadEvent evt)
        {
            if (waitsData == null)
            {
                waitsData = evt.Waits;
            }
            else
            {
                waitsData.Merge(evt.Waits);
            }
        }

        private void InternalAdd(DiskPerfWorkloadEvent evt)
        {
            if (diskPerfData == null)
            {
                diskPerfData = evt.DiskPerf;
            }
            else
            {
                diskPerfData.Merge(evt.DiskPerf);
            }
        }

        private void InternalAdd(CounterWorkloadEvent evt)
        {
            if (counterData == null)
            {
                counterData = new DataTable();

                _ = counterData.Columns.Add("event_time", typeof(DateTime));
                _ = counterData.Columns.Add("counter_name", typeof(string));
                _ = counterData.Columns.Add("counter_value", typeof(float));
            }

            foreach(var cntr in evt.Counters.Keys)
            {
                var row = counterData.NewRow();

                row.SetField("event_time", evt.StartTime);
                row.SetField("counter_name", cntr.ToString());
                row.SetField("counter_value", evt.Counters[cntr]);

                counterData.Rows.Add(row);
            }
            
        }

        private void InternalAdd(ExecutionWorkloadEvent evt)
        {
            if (rawData == null)
            {
                PrepareDataTables();
                PrepareDictionaries();
            }

            var norm = normalizer.NormalizeSqlText(evt.Text, (int)evt.SPID);

            string normSql;
            if (norm != null)
            {
                normSql = norm.NormalizedText;
            }
            else
            {
                return;
            }

            if (normSql == null)
            {
                return;
            }

            var hash = normalizer.GetHashCode(normSql);

            if (!normalizedQueries.ContainsKey(hash))
            {
                normalizedQueries.Add(hash, new NormalizedQuery { Hash = hash, NormalizedText = normSql, ExampleText = evt.Text });
            }

            var appId = -1;
            if (evt.ApplicationName != null && !applications.TryGetValue(evt.ApplicationName, out appId))
            {
                applications.Add(evt.ApplicationName, appId = applications.Count);
            }

            var dbId = -1;
            if (evt.DatabaseName != null && !databases.TryGetValue(evt.DatabaseName, out dbId))
            {
                databases.Add(evt.DatabaseName, dbId = databases.Count);
            }

            var hostId = -1;
            if (evt.HostName != null && !hosts.TryGetValue(evt.HostName, out hostId))
            {
                hosts.Add(evt.HostName, hostId = hosts.Count);
            }

            var loginId = -1;
            if (evt.LoginName != null && !logins.TryGetValue(evt.LoginName, out loginId))
            {
                logins.Add(evt.LoginName, loginId = logins.Count);
            }

            var theKey = new ExecutionDetailKey()
            {
                Sql_hash = hash,
                Application_id = appId,
                Database_id = dbId,
                Host_id = hostId,
                Login_id = loginId
            };

            // Update pre-aggregated stats - O(n_distinct_keys) memory instead of O(n_events)
            rawData.AddOrUpdate(
                theKey,
                _ =>
                {
                    var stats = new AggregatedExecutionStats();
                    stats.Accumulate(evt);
                    return stats;
                },
                (_, existing) =>
                {
                    existing.Accumulate(evt);
                    return existing;
                });
        }

        public void Stop()
        {
            try
            {
                WriteToServer(lastEventTime);
            }
            catch (Exception e)
            {
                // duplicate key errors might be thrown at this time
                // that's expected if trying to upload to the same
                // interval already uploaded and new queries with the 
                // same hash have been captured
                if(!e.Message.Contains("Violation of PRIMARY KEY"))
                {
                    throw;
                }
            }
            stopped = true;
            // Wake up any Add() calls that may be blocked waiting for queue space
            lock (_internalQueueLock)
            {
                Monitor.PulseAll(_internalQueueLock);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private void WriteToServer(DateTime intervalTime)
        {
            logger.Trace("Writing Workload Analysis data");

            using (var conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString();
                conn.Open();

                if (!TargetTableCreated)
                {
                    CreateTargetTables();
                    TargetTableCreated = true;
                }

                var tran = conn.BeginTransaction();

                try
                {
                    var current_interval_id = 0;
                    if(WriteDetail)
                    {
                        current_interval_id = CreateInterval(conn, tran, intervalTime);
                    }

                    WriteDictionary(applications, conn, tran, "applications");
                    WriteDictionary(databases, conn, tran, "databases");
                    WriteDictionary(hosts, conn, tran, "hosts");
                    WriteDictionary(logins, conn, tran, "logins");

                    if (rawData == null)
                    {
                        PrepareDataTables();
                    }

                    lock (rawData)
                    {
                        if (WriteSummary)
                        {
                            WriteExecutionSummary(conn, tran);
                        }

                        if (WriteDetail)
                        {
                            WriteExecutionDetails(conn, tran, current_interval_id);
                        }
                    }
                    rawData.Clear();

                    if (WriteDetail)
                    {
                        WriteNormalizedQueries(normalizedQueries, conn, tran);
                        WriteExecutionErrors(conn, tran, current_interval_id);
                        WritePerformanceCounters(conn, tran, current_interval_id);
                        WriteWaitsData(conn, tran, current_interval_id);
                        WriteDiskPerf(conn, tran, current_interval_id);
                    }

                    tran.Commit();
                }
                catch(Exception)
                {
                    tran.Rollback();
                    throw;
                }

            }

        }

        private void WriteWaitsData(SqlConnection conn, SqlTransaction tran, int current_interval_id)
        {
            if (waitsData == null)
            {
                return;
            }

            lock (waitsData)
            {
                using (var bulkCopy = new System.Data.SqlClient.SqlBulkCopy(conn,
                                                SqlBulkCopyOptions.KeepIdentity |
                                                SqlBulkCopyOptions.FireTriggers |
                                                SqlBulkCopyOptions.CheckConstraints |
                                                SqlBulkCopyOptions.TableLock,
                                                tran))
                {

                    bulkCopy.DestinationTableName = "[" + ConnectionInfo.SchemaName + "].[WaitStats]";
                    bulkCopy.BatchSize = 1000;
                    bulkCopy.BulkCopyTimeout = 300;

                    var Table = from t in waitsData.AsEnumerable()
                                group t by new
                                {
                                    wait_type = t.Field<string>("wait_type")
                                }
                                into grp
                                select new
                                {
                                    interval_id = current_interval_id,

                                    grp.Key.wait_type,

                                    wait_sec = grp.Sum(t => t.Field<double>("wait_sec")),
                                    resource_sec = grp.Sum(t => t.Field<double>("resource_sec")),
                                    signal_sec = grp.Sum(t => t.Field<double>("signal_sec")),
                                    wait_count = grp.Sum(t => t.Field<double>("wait_count"))
                                };

                    using(var dt = DataUtils.ToDataTable(Table))
                    {
                        bulkCopy.WriteToServer(dt);
                    }
                    
                    logger.Info("Wait stats written");
                }
                waitsData.Dispose();
                waitsData = null;
            }
        }

        private void WriteDiskPerf(SqlConnection conn, SqlTransaction tran, int current_interval_id)
        {
            if (diskPerfData == null)
            {
                return;
            }

            lock (diskPerfData)
            {
                using (var bulkCopy = new System.Data.SqlClient.SqlBulkCopy(conn,
                                                SqlBulkCopyOptions.KeepIdentity |
                                                SqlBulkCopyOptions.FireTriggers |
                                                SqlBulkCopyOptions.CheckConstraints |
                                                SqlBulkCopyOptions.TableLock,
                                                tran))
                {

                    bulkCopy.DestinationTableName = "[" + ConnectionInfo.SchemaName + "].[DiskPerf]";
                    bulkCopy.BatchSize = 1000;
                    bulkCopy.BulkCopyTimeout = 300;

                    var Table = from t in diskPerfData.AsEnumerable()
                                group t by new
                                {
                                    database_name = t.Field<string>("database_name"),
                                    physical_filename = t.Field<string>("physical_filename"),
                                    logical_filename = t.Field<string>("logical_filename"),
                                    file_type = t.Field<string>("file_type"),
                                    volume_mount_point = t.Field<string>("volume_mount_point"),
                                }
                                into grp
                                select new
                                {
                                    interval_id = current_interval_id,

                                    grp.Key.database_name,
                                    grp.Key.physical_filename,
                                    grp.Key.logical_filename,
                                    grp.Key.file_type,
                                    grp.Key.volume_mount_point,

                                    read_latency_ms = grp.Average(t => t.Field<double>("read_latency_ms")),
                                    reads = grp.Sum(t => t.Field<double>("reads")),
                                    read_bytes = grp.Sum(t => t.Field<double>("read_bytes")),
                                    write_latency_ms = grp.Average(t => t.Field<double>("write_latency_ms")),
                                    writes = grp.Sum(t => t.Field<double>("writes")),
                                    write_bytes = grp.Sum(t => t.Field<double>("write_bytes")),

                                    cum_read_latency_ms = grp.Max(t => t.Field<double?>("cum_read_latency_ms")),
                                    cum_reads = grp.Max(t => t.Field<double?>("cum_reads")),
                                    cum_read_bytes = grp.Max(t => t.Field<double?>("cum_read_bytes")),
                                    cum_write_latency_ms = grp.Max(t => t.Field<double?>("cum_write_latency_ms")),
                                    cum_writes = grp.Max(t => t.Field<double?>("cum_writes")),
                                    cum_write_bytes = grp.Max(t => t.Field<double?>("cum_write_bytes"))
                                };

                    using (var dt = DataUtils.ToDataTable(Table))
                    {
                        bulkCopy.WriteToServer(dt);
                    }

                    logger.Info("Disk perf written");
                }
                diskPerfData.Dispose();
                diskPerfData = null;
            }
        }

        private void WritePerformanceCounters(SqlConnection conn, SqlTransaction tran, int current_interval_id)
        {
            if (counterData == null)
            {
                return;
            }

            lock (counterData)
            {
                using (var bulkCopy = new System.Data.SqlClient.SqlBulkCopy(conn,
                                                SqlBulkCopyOptions.KeepIdentity |
                                                SqlBulkCopyOptions.FireTriggers |
                                                SqlBulkCopyOptions.CheckConstraints |
                                                SqlBulkCopyOptions.TableLock,
                                                tran))
                {

                    bulkCopy.DestinationTableName = "[" + ConnectionInfo.SchemaName + "].[PerformanceCounters]";
                    bulkCopy.BatchSize = 1000;
                    bulkCopy.BulkCopyTimeout = 300;

                    var Table = from t in counterData.AsEnumerable()
                                group t by new
                                {
                                    counter_name = t.Field<string>("counter_name")
                                }
                                into grp
                                select new
                                {
                                    interval_id = current_interval_id,

                                    grp.Key.counter_name,

                                    min_counter_value = grp.Min(t => t.Field<float>("counter_value")),
                                    max_counter_value = grp.Max(t => t.Field<float>("counter_value")),
                                    avg_counter_value = grp.Average(t => t.Field<float>("counter_value"))
                                };

                    using (var dt = DataUtils.ToDataTable(Table))
                    {
                        bulkCopy.WriteToServer(dt);
                    }
                    logger.Info("Performance counters written");
                }
                counterData.Dispose();
                counterData = null;
            }
        }

        private void WriteExecutionSummary(SqlConnection conn, SqlTransaction tran)
        {
            // create temporary table for uploading data
            var sql = $@"
                IF OBJECT_ID('tempdb..#WorkloadSummary') IS NOT NULL 
                    DROP TABLE #WorkloadSummary;
                    
                SELECT TOP(0) * INTO #WorkloadSummary FROM [{ConnectionInfo.SchemaName}].WorkloadSummary;
            ";
            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tran;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }

            // bulk copy data to temp table
            using (var bulkCopy = new System.Data.SqlClient.SqlBulkCopy(conn,
                                            SqlBulkCopyOptions.KeepIdentity |
                                            SqlBulkCopyOptions.FireTriggers |
                                            SqlBulkCopyOptions.CheckConstraints |
                                            SqlBulkCopyOptions.TableLock,
                                            tran))
            {

                bulkCopy.DestinationTableName = "#WorkloadSummary";
                bulkCopy.BatchSize = 1000;
                bulkCopy.BulkCopyTimeout = 300;

                // Group pre-aggregated stats by (app, db, host, login) - ignoring sql_hash
                var Table = from kvp in rawData
                            let t = kvp.Key
                            let stats = kvp.Value
                            where stats.ExecutionCount > 0
                            group stats by new
                            {
                                application_id = t.Application_id,
                                database_id = t.Database_id,
                                host_id = t.Host_id,
                                login_id = t.Login_id
                            }
                            into grp
                            select new
                            {
                                grp.Key.application_id,
                                grp.Key.database_id,
                                grp.Key.host_id,
                                grp.Key.login_id,

                                min_cpu_us = grp.Min(s => s.MinCpu_us),
                                max_cpu_us = grp.Max(s => s.MaxCpu_us),
                                sum_cpu_us = grp.Sum(s => s.SumCpu_us),

                                min_reads = grp.Min(s => s.MinReads),
                                max_reads = grp.Max(s => s.MaxReads),
                                sum_reads = grp.Sum(s => s.SumReads),

                                min_writes = grp.Min(s => s.MinWrites),
                                max_writes = grp.Max(s => s.MaxWrites),
                                sum_writes = grp.Sum(s => s.SumWrites),

                                min_duration_us = grp.Min(s => s.MinDuration_us),
                                max_duration_us = grp.Max(s => s.MaxDuration_us),
                                sum_duration_us = grp.Sum(s => s.SumDuration_us),

                                min_execution_date = grp.Min(s => s.MinEventTime),
                                max_execution_date = grp.Max(s => s.MaxEventTime),

                                execution_count = grp.Sum(s => s.ExecutionCount)
                            };

                using (var reader = ObjectReader.Create(Table, "application_id", "database_id", "host_id", "login_id", "min_cpu_us", "max_cpu_us", "sum_cpu_us", "min_reads", "max_reads", "sum_reads", "min_writes", "max_writes", "sum_writes", "min_duration_us", "max_duration_us", "sum_duration_us", "min_execution_date", "max_execution_date", "execution_count"))
                {
                    bulkCopy.WriteToServer(reader);
                }

            }

            var affectedRows = 0;
            // merge with existing data

            sql = $@"
                UPDATE WS
                SET min_cpu_us = CASE WHEN T.min_cpu_us < WS.min_cpu_us THEN T.min_cpu_us ELSE WS.min_cpu_us END,
                    max_cpu_us = CASE WHEN T.max_cpu_us > WS.max_cpu_us THEN T.max_cpu_us ELSE WS.max_cpu_us END,
                    sum_cpu_us += T.sum_cpu_us,
                    min_reads  = CASE WHEN T.min_reads < WS.min_reads THEN T.min_reads ELSE WS.min_reads END,
                    max_reads  = CASE WHEN T.max_reads > WS.max_reads THEN T.max_reads ELSE WS.max_reads END,
                    sum_reads  += T.sum_reads,
                    min_writes = CASE WHEN T.min_writes < WS.min_writes THEN T.min_writes ELSE WS.min_writes END,
                    max_writes = CASE WHEN T.max_writes > WS.max_writes THEN T.max_writes ELSE WS.max_writes END,
                    sum_writes += T.sum_writes,
                    min_duration_us = CASE WHEN T.min_duration_us < WS.min_duration_us THEN T.min_duration_us ELSE WS.min_duration_us END,
                    max_duration_us = CASE WHEN T.max_duration_us > WS.max_duration_us THEN T.max_duration_us ELSE WS.max_duration_us END,
                    sum_duration_us += T.sum_duration_us,
                    min_execution_date = CASE WHEN T.min_execution_date < WS.min_execution_date THEN T.min_execution_date ELSE WS.min_execution_date END,
                    max_execution_date = CASE WHEN T.max_execution_date > WS.max_execution_date THEN T.max_execution_date ELSE WS.max_execution_date END,
                    execution_count += T.execution_count
                FROM [{ConnectionInfo.SchemaName}].WorkloadSummary AS WS
                INNER JOIN #WorkloadSummary AS T
                    ON  T.application_id = WS.application_id
                    AND T.database_id    = WS.database_id
                    AND T.host_id        = WS.host_id
                    AND T.login_id       = WS.login_id;
            ";
            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tran;
                cmd.CommandText = sql;
                affectedRows += cmd.ExecuteNonQuery();
            }

            sql = $@"
                INSERT INTO [{ConnectionInfo.SchemaName}].WorkloadSummary 
                SELECT * 
                FROM #WorkloadSummary AS T
                WHERE NOT EXISTS (
                    SELECT *
                    FROM [{ConnectionInfo.SchemaName}].WorkloadSummary AS WS
                    WHERE   T.application_id = WS.application_id
                        AND T.database_id    = WS.database_id
                        AND T.host_id        = WS.host_id
                        AND T.login_id       = WS.login_id
                );
            ";
            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tran;
                cmd.CommandText = sql;
                affectedRows += cmd.ExecuteNonQuery();
            }

            logger.Info($"Summary info written ({affectedRows} rows)");
        }

        private void WriteExecutionDetails(SqlConnection conn, SqlTransaction tran, int current_interval_id)
        {
            long numRows;

            using (var bulkCopy = new System.Data.SqlClient.SqlBulkCopy(conn,
                                            SqlBulkCopyOptions.KeepIdentity |
                                            SqlBulkCopyOptions.FireTriggers |
                                            SqlBulkCopyOptions.CheckConstraints |
                                            SqlBulkCopyOptions.TableLock,
                                            tran))
            {

                bulkCopy.DestinationTableName = "[" + ConnectionInfo.SchemaName + "].[WorkloadDetails]";
                bulkCopy.BatchSize = 1000;
                bulkCopy.BulkCopyTimeout = 300;

                // Each rawData entry is already pre-aggregated per (sql_hash, app, db, host, login)
                var Table = from kvp in rawData
                            let t = kvp.Key
                            let stats = kvp.Value
                            where stats.ExecutionCount > 0
                            select new
                            {
                                interval_id = current_interval_id,

                                sql_hash = t.Sql_hash,
                                application_id = t.Application_id,
                                database_id = t.Database_id,
                                host_id = t.Host_id,
                                login_id = t.Login_id,

                                avg_cpu_us = stats.ExecutionCount > 0 && stats.SumCpu_us.HasValue ? (long?)(stats.SumCpu_us.Value / stats.ExecutionCount) : (long?)null,
                                min_cpu_us = stats.MinCpu_us,
                                max_cpu_us = stats.MaxCpu_us,
                                sum_cpu_us = stats.SumCpu_us,

                                avg_reads = stats.ExecutionCount > 0 && stats.SumReads.HasValue ? (long?)(stats.SumReads.Value / stats.ExecutionCount) : (long?)null,
                                min_reads = stats.MinReads,
                                max_reads = stats.MaxReads,
                                sum_reads = stats.SumReads,

                                avg_writes = stats.ExecutionCount > 0 && stats.SumWrites.HasValue ? (long?)(stats.SumWrites.Value / stats.ExecutionCount) : (long?)null,
                                min_writes = stats.MinWrites,
                                max_writes = stats.MaxWrites,
                                sum_writes = stats.SumWrites,

                                avg_duration_us = stats.ExecutionCount > 0 && stats.SumDuration_us.HasValue ? (long?)(stats.SumDuration_us.Value / stats.ExecutionCount) : (long?)null,
                                min_duration_us = stats.MinDuration_us,
                                max_duration_us = stats.MaxDuration_us,
                                sum_duration_us = stats.SumDuration_us,

                                execution_count = (long)stats.ExecutionCount
                            };

                using (var reader = ObjectReader.Create(Table, "interval_id", "sql_hash", "application_id", "database_id", "host_id", "login_id", "avg_cpu_us", "min_cpu_us", "max_cpu_us", "sum_cpu_us", "avg_reads", "min_reads", "max_reads", "sum_reads", "avg_writes", "min_writes", "max_writes", "sum_writes", "avg_duration_us", "min_duration_us", "max_duration_us", "sum_duration_us", "execution_count"))
                {
                    bulkCopy.WriteToServer(reader);
                }
                numRows = rawData.Sum(x => x.Value.ExecutionCount);
                logger.Info($"{numRows} rows aggregated");
                numRows = rawData.Count();
                logger.Info($"{numRows} rows written");
            }
        }

		private void WriteExecutionErrors(SqlConnection conn, SqlTransaction tran, int current_interval_id)
		{

			if (errorData == null)
            {
                PrepareDataTables();
            }

            lock (errorData)
			{
				using (var bulkCopy = new System.Data.SqlClient.SqlBulkCopy(conn,
												SqlBulkCopyOptions.KeepIdentity |
												SqlBulkCopyOptions.FireTriggers |
												SqlBulkCopyOptions.CheckConstraints |
												SqlBulkCopyOptions.TableLock,
												tran))
				{

					bulkCopy.DestinationTableName = "[" + ConnectionInfo.SchemaName + "].[Errors]";
					bulkCopy.BatchSize = 1000;
					bulkCopy.BulkCopyTimeout = 300;

					var Table = from t in errorData.AsEnumerable()
								group t by new
								{
                                    type = t.Field<int>("type"),
									message = t.Field<string>("message")
								}
								into grp
								select new
								{
									interval_id = current_interval_id,
                                    error_type = ((WorkloadEvent.EventType)grp.Key.type).ToString(),
									grp.Key.message,
									error_count = grp.Count()
								};

					bulkCopy.WriteToServer(DataUtils.ToDataTable(Table));
				}
				errorData.Rows.Clear();
			}
		}

		private void WriteDictionary(Dictionary<string, int> values, SqlConnection conn, SqlTransaction tran, string name)
        {

            // create a temporary table

            var sql = @"
                SELECT TOP(0) *
                INTO #{0}
                FROM [{1}].[{0}];
            ";
            sql = string.Format(sql, name, ConnectionInfo.SchemaName);

            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tran;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }

            // bulk insert into temporary
            using (var bulkCopy = new System.Data.SqlClient.SqlBulkCopy(conn,
                                                                SqlBulkCopyOptions.KeepIdentity |
                                                                SqlBulkCopyOptions.FireTriggers |
                                                                SqlBulkCopyOptions.CheckConstraints |
                                                                SqlBulkCopyOptions.TableLock,
                                                                tran))
            {

                bulkCopy.DestinationTableName = "#" + name;
                bulkCopy.BatchSize = 1000;
                bulkCopy.BulkCopyTimeout = 300;
                bulkCopy.WriteToServer(DataUtils.ToDataTable(from t in values select new { t.Value, t.Key }));

            }

            // merge new data

            sql = @"
                INSERT INTO [{1}].[{0}s]
                SELECT *
                FROM #{0}s AS src
                WHERE NOT EXISTS (
                    SELECT *
                    FROM [{1}].[{0}s] AS dst 
                    WHERE dst.[{0}_id] = src.[{0}_id]
                );
            ";
            sql = string.Format(sql, name.Substring(0, name.Length - 1), ConnectionInfo.SchemaName);

            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tran;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }
        }

        private void WriteNormalizedQueries(Dictionary<long, NormalizedQuery> values, SqlConnection conn, SqlTransaction tran)
        {
            // create a temporary table

            var sql = @"
                SELECT TOP(0) *
                INTO #NormalizedQueries
                FROM [{0}].[NormalizedQueries];
            ";
            sql = string.Format(sql, ConnectionInfo.SchemaName);

            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tran;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }

            // bulk insert into temporary
            using (var bulkCopy = new System.Data.SqlClient.SqlBulkCopy(conn,
                                                                SqlBulkCopyOptions.KeepIdentity |
                                                                SqlBulkCopyOptions.FireTriggers |
                                                                SqlBulkCopyOptions.CheckConstraints |
                                                                SqlBulkCopyOptions.TableLock,
                                                                tran))
            {

                bulkCopy.DestinationTableName = "#NormalizedQueries";
                bulkCopy.BatchSize = 1000;
                bulkCopy.BulkCopyTimeout = 300;
                bulkCopy.WriteToServer(DataUtils.ToDataTable(from t in values where t.Value != null select new { t.Value.Hash, t.Value.NormalizedText, t.Value.ExampleText }));

            }

            // merge new data

            sql = @"
                INSERT INTO [{0}].[NormalizedQueries]
                SELECT *
                FROM #NormalizedQueries AS src
                WHERE NOT EXISTS (
                    SELECT *
                    FROM [{0}].[NormalizedQueries] AS dst 
                    WHERE dst.[sql_hash] = src.[sql_hash]
                );
            ";
            sql = string.Format(sql, ConnectionInfo.SchemaName);

            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tran;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }

            // Erase from memory all the normalized queries 
            // already written to the database. This should reduce
            // the memory footprint quite a lot
            foreach(var hash in values.Keys.ToList())
            {
                values[hash] = null;
            }
            // Run the Garbage Collector in a separate task
            _ = Task.Factory.StartNew(() => InvokeGC());
        }

        private void InvokeGC()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        private int CreateInterval(SqlConnection conn, SqlTransaction tran, DateTime intervalTime)
        {
            var sql = @"
                UPDATE [{0}].[Intervals]
                SET  end_time = @end_time
                    ,duration_minutes = @duration_minutes
                WHERE interval_id = @interval_id;
                
                IF @@ROWCOUNT = 0
                    INSERT INTO [{0}].[Intervals] (interval_id, end_time, duration_minutes) 
                    VALUES (@interval_id, @end_time, @duration_minutes); 
            ";
            sql = string.Format(sql, ConnectionInfo.SchemaName);

            // interval id is the number of seconds since 01/01/2000
            var interval_id = (int)intervalTime.Subtract(DateTime.MinValue.AddYears(1999)).TotalSeconds;

            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tran;
                cmd.CommandText = sql;
                _ = cmd.Parameters.AddWithValue("@interval_id", interval_id);
                _ = cmd.Parameters.AddWithValue("@end_time", intervalTime);
                _ = cmd.Parameters.AddWithValue("@duration_minutes", Interval);
                _ = cmd.ExecuteNonQuery();
            }

            // If this the first interval of the analysis, write
            // a marker interval with duration = 0 
            if (!FirstIntervalWritten)
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.Transaction = tran;
                    cmd.CommandText = sql;
                    _ = cmd.Parameters.AddWithValue("@interval_id", interval_id - 1);
                    _ = cmd.Parameters.AddWithValue("@end_time", intervalTime.AddSeconds(-1));
                    _ = cmd.Parameters.AddWithValue("@duration_minutes", 0);
                    _ = cmd.ExecuteNonQuery();
                    FirstIntervalWritten = true;
                }
            }

            return interval_id;
        }

        private void PrepareDataTables()
        {
            rawData = new ConcurrentDictionary<ExecutionDetailKey, AggregatedExecutionStats>();
			errorData = new DataTable();
            _ = errorData.Columns.Add("type", typeof(int));
            _ = errorData.Columns.Add("message", typeof(string));
		}

        private void PrepareDictionaries()
        {
			CreateTargetDatabase();

			using (var conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString();
                conn.Open();

                var sql = string.Format(@"SELECT * FROM [{0}].[Applications]",ConnectionInfo.SchemaName);
                AddAllRows(conn, sql, applications);

                sql = string.Format(@"SELECT * FROM [{0}].[Databases]", ConnectionInfo.SchemaName);
                AddAllRows(conn, sql, databases);

                sql = string.Format(@"SELECT * FROM [{0}].[Hosts]", ConnectionInfo.SchemaName);
                AddAllRows(conn, sql, hosts);

                sql = string.Format(@"SELECT * FROM [{0}].[Logins]", ConnectionInfo.SchemaName);
                AddAllRows(conn, sql, logins);
            }
        }

        private void AddAllRows(SqlConnection conn, string sql,  Dictionary<string, int> d)
        {
            try
            {

                using (var adapter = new SqlDataAdapter(sql, conn))
                {
                    using (var ds = new DataSet())
                    {
                        _ = adapter.Fill(ds);
                        var dt = ds.Tables[0];
                        foreach (DataRow dr in dt.Rows)
                        {
                            d.Add((string)dr[1], (int)dr[0]);
                        }
                    }
                }
            }
            catch(SqlException e)
            {
                logger.Trace("Unable to read saved classifiers from the analysis database: {0}", e.Message);
            }
            catch(Exception e)
            {
                logger.Error(e.Message);
                throw;
            }
        }

        protected void CreateTargetTables()
        {
			CreateTargetDatabase();

			var sql = File.ReadAllText(WorkloadController.BaseLocation + "\\Consumer\\Analysis\\DatabaseSchema.sql");

            sql = sql.Replace("{DatabaseName}", ConnectionInfo.DatabaseName);
            sql = sql.Replace("{SchemaName}", ConnectionInfo.SchemaName);

            using (var conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString();
                conn.Open();
                conn.ChangeDatabase(ConnectionInfo.DatabaseName);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = sql;
                    _ = cmd.ExecuteNonQuery();
                }

                sql = "IF OBJECT_ID('dbo.createAnalysisView') IS NULL EXEC('CREATE PROCEDURE dbo.createAnalysisView AS RETURN 0')";
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = sql;
                    _ = cmd.ExecuteNonQuery();
                }

                sql = File.ReadAllText(WorkloadController.BaseLocation + "\\Consumer\\Analysis\\createAnalysisView.sql");
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = sql;
                    _ = cmd.ExecuteNonQuery();
                }

                // Invoke the stored procedure to create the workload comparison view
                sql = @"
                    DECLARE @name1 sysname, @name2 sysname;

                    SELECT @name1 = [1], @name2 = [2]
                    FROM (
                        SELECT TOP(2) OBJECT_SCHEMA_NAME(object_id) AS schema_name, ROW_NUMBER() OVER (ORDER BY create_date DESC) AS RN
                        FROM sys.tables
                        WHERE name = 'WorkloadDetails'
                        ORDER BY create_date DESC
                    ) AS src
                    PIVOT( MIN(schema_name) FOR RN IN ([1], [2])) AS p;

                    SELECT @name1 ,@name2

                    IF OBJECT_ID(@name1 + '.WorkloadDetails') IS NOT NULL OR OBJECT_ID(@name2 + '.WorkloadDetails') IS NOT NULL
                    BEGIN
                        EXEC createAnalysisView @name1, @name2;
                    END
                ";
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = sql;
                    _ = cmd.ExecuteNonQuery();
                }
            }
        }

		protected void CreateTargetDatabase()
		{

			try
			{
                var databaseName = ConnectionInfo.DatabaseName;
                using (var conn = new SqlConnection())
				{
                    // create a new connection to the target server 
                    // for the analysis database, on the master db
                    // then create the target database if not available
                    var ci = new SqlConnectionInfo(ConnectionInfo);
                    ci.DatabaseName = "master";
					conn.ConnectionString = ConnectionInfo.ConnectionString();
					conn.Open();

					using (var cmd = conn.CreateCommand())
					{
						var createDb = @"
						IF DB_ID(@name) IS NULL
						BEGIN
						    DECLARE @sql nvarchar(max); 
							SET @sql = N'CREATE DATABASE ' + QUOTENAME(@name);
							EXEC sp_executesql @sql;
						END
					";
						cmd.CommandText = createDb;
                        _ = cmd.Parameters.AddWithValue("@name", databaseName);
                        _ = cmd.ExecuteNonQuery();
					}
				}
			}
            catch(Exception e) {
                logger.Warn("Unable to create the target database for the analysis", e.Message);
            }

		}

        public void Dispose()
        {
            rawData?.Clear();
            errorData?.Dispose();
            counterData?.Dispose();
            waitsData?.Dispose();
        }

        internal class ExecutionDetailKey : IEquatable<ExecutionDetailKey>
        {
            public long Sql_hash { get; set; }
            public int Application_id { get; set; }
            public int Database_id { get; set; }
            public int Host_id { get; set; }
            public int Login_id { get; set; }

            public override int GetHashCode()
            {
                var hash = 497;
                unchecked
                {
                    hash = (hash * 17) + Sql_hash.GetHashCode();
                    hash = (hash * 17) + Application_id.GetHashCode();
                    hash = (hash * 17) + Database_id.GetHashCode();
                    hash = (hash * 17) + Host_id.GetHashCode();
                    hash = (hash * 17) + Login_id.GetHashCode();
                }
                return hash;
            }

            public override bool Equals(object other)
            {
                return Equals(other as ExecutionDetailKey);
            }

            public bool Equals(ExecutionDetailKey other)
            {
                return other != null
                    && Sql_hash.Equals(other.Sql_hash)
                    && Application_id.Equals(other.Application_id)
                    && Database_id.Equals(other.Database_id)
                    && Host_id.Equals(other.Host_id)
                    && Login_id.Equals(other.Login_id);
            }
        }

        /// <summary>
        /// Pre-aggregated execution statistics per query key.
        /// Stores running min/max/sum/count so that memory usage is O(distinct_queries)
        /// rather than O(total_events), preventing unbounded memory growth on long workloads.
        /// </summary>
        internal class AggregatedExecutionStats
        {
            public long ExecutionCount { get; private set; }
            public DateTime MinEventTime { get; private set; } = DateTime.MaxValue;
            public DateTime MaxEventTime { get; private set; } = DateTime.MinValue;

            public long? MinCpu_us { get; private set; }
            public long? MaxCpu_us { get; private set; }
            public long? SumCpu_us { get; private set; }

            public long? MinReads { get; private set; }
            public long? MaxReads { get; private set; }
            public long? SumReads { get; private set; }

            public long? MinWrites { get; private set; }
            public long? MaxWrites { get; private set; }
            public long? SumWrites { get; private set; }

            public long? MinDuration_us { get; private set; }
            public long? MaxDuration_us { get; private set; }
            public long? SumDuration_us { get; private set; }

            public void Accumulate(ExecutionWorkloadEvent evt)
            {
                ExecutionCount++;

                if (evt.StartTime < MinEventTime) MinEventTime = evt.StartTime;
                if (evt.StartTime > MaxEventTime) MaxEventTime = evt.StartTime;

                if (evt.CPU.HasValue)
                {
                    MinCpu_us = MinCpu_us.HasValue ? Math.Min(MinCpu_us.Value, evt.CPU.Value) : evt.CPU.Value;
                    MaxCpu_us = MaxCpu_us.HasValue ? Math.Max(MaxCpu_us.Value, evt.CPU.Value) : evt.CPU.Value;
                    SumCpu_us = (SumCpu_us ?? 0) + evt.CPU.Value;
                }

                if (evt.Reads.HasValue)
                {
                    MinReads = MinReads.HasValue ? Math.Min(MinReads.Value, evt.Reads.Value) : evt.Reads.Value;
                    MaxReads = MaxReads.HasValue ? Math.Max(MaxReads.Value, evt.Reads.Value) : evt.Reads.Value;
                    SumReads = (SumReads ?? 0) + evt.Reads.Value;
                }

                if (evt.Writes.HasValue)
                {
                    MinWrites = MinWrites.HasValue ? Math.Min(MinWrites.Value, evt.Writes.Value) : evt.Writes.Value;
                    MaxWrites = MaxWrites.HasValue ? Math.Max(MaxWrites.Value, evt.Writes.Value) : evt.Writes.Value;
                    SumWrites = (SumWrites ?? 0) + evt.Writes.Value;
                }

                if (evt.Duration.HasValue)
                {
                    MinDuration_us = MinDuration_us.HasValue ? Math.Min(MinDuration_us.Value, evt.Duration.Value) : evt.Duration.Value;
                    MaxDuration_us = MaxDuration_us.HasValue ? Math.Max(MaxDuration_us.Value, evt.Duration.Value) : evt.Duration.Value;
                    SumDuration_us = (SumDuration_us ?? 0) + evt.Duration.Value;
                }
            }
        }
    }
}

