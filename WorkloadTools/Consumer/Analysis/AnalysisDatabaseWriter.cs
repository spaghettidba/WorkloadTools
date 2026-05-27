using System;
using System.IO;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics.PerformanceData;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

using FastMember;

using NLog;

using WorkloadTools.Util;

using NFX.DataAccess;

namespace WorkloadTools.Consumer.Analysis
{
    public abstract class AnalysisDatabaseWriter
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public SqlConnectionInfo ConnectionInfo { get; set; }
        public int Interval { get; set; }
        public int MaximumWriteRetries { get; set; }
        public bool TruncateTo4000 { get; set; }
        public bool TruncateTo1024 { get; set; }
        public bool WriteDetail { get; set; } = true;
        public bool WriteSummary { get; set; } = true;

        protected bool TargetTableCreated = false;
        protected bool FirstIntervalWritten = false;
        public volatile int LastWrittenIntervalId = -1;

        public WorkloadData Data { get; set; }

        protected AnalysisDatabaseWriter() { }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void WriteToServer(DateTime intervalTime)
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
                    if (WriteDetail)
                    {
                        current_interval_id = CreateInterval(conn, tran, intervalTime);
                    }

                    WriteDictionary(Data.Applications, conn, tran, "applications");
                    WriteDictionary(Data.Databases, conn, tran, "databases");
                    WriteDictionary(Data.Hosts, conn, tran, "hosts");
                    WriteDictionary(Data.Logins, conn, tran, "logins");

                    

                    lock (Data)
                    {
                        if (WriteSummary)
                        {
                            WriteExecutionSummary(conn, tran);
                        }

                        if (WriteDetail)
                        {
                            WriteExecutionDetails(conn, tran, current_interval_id);
                        }

                        Data.ClearRawData();
                    }

                    if (WriteDetail)
                    {
                        WriteNormalizedQueries(conn, tran, Data.NormalizedQueries);
                        WriteExecutionErrors(conn, tran, current_interval_id);
                        WritePerformanceCounters(conn, tran, current_interval_id);
                        WriteWaitsData(conn, tran, current_interval_id);
                        WriteDiskPerf(conn, tran, current_interval_id);
                    }

                    tran.Commit();
                    if (WriteDetail)
                    {
                        LastWrittenIntervalId = current_interval_id;
                    }
                }
                catch (Exception)
                {
                    tran.Rollback();
                    throw;
                }

            }

        }

        private void WriteWaitsData(SqlConnection conn, SqlTransaction tran, int current_interval_id)
        {
            if (Data.WaitsData == null)
            {
                return;
            }

            lock (Data.WaitsData)
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

                    var Table = from t in Data.WaitsData.AsEnumerable()
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

                    using (var dt = DataUtils.ToDataTable(Table))
                    {
                        bulkCopy.WriteToServer(dt);
                    }

                    logger.Info("Wait stats written");
                }
                Data.WaitsData.Dispose();
                Data.WaitsData = null;
            }
        }

        private void WriteDiskPerf(SqlConnection conn, SqlTransaction tran, int current_interval_id)
        {
            if (Data.DiskPerfData == null)
            {
                return;
            }

            lock (Data.DiskPerfData)
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

                    var Table = from t in Data.DiskPerfData.AsEnumerable()
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
                Data.DiskPerfData.Dispose();
                Data.DiskPerfData = null;
            }
        }

        private void WritePerformanceCounters(SqlConnection conn, SqlTransaction tran, int current_interval_id)
        {
            if (Data.CounterData == null)
            {
                return;
            }

            lock (Data.CounterData)
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

                    var Table = from t in Data.CounterData.AsEnumerable()
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
                Data.CounterData.Dispose();
                Data.CounterData = null;
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

                var Table = from t in Data.RawData.Keys
                            from v in Data.RawData[t]
                            group new
                            {
                                v.Cpu_us,
                                v.Duration_us,
                                v.Event_time,
                                v.Reads,
                                v.Writes
                            }
                            by new
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

                                min_cpu_us = grp.Min(v => v.Cpu_us),
                                max_cpu_us = grp.Max(v => v.Cpu_us),
                                sum_cpu_us = grp.Sum(v => v.Cpu_us),

                                min_reads = grp.Min(v => v.Reads),
                                max_reads = grp.Max(v => v.Reads),
                                sum_reads = grp.Sum(v => v.Reads),

                                min_writes = grp.Min(v => v.Writes),
                                max_writes = grp.Max(v => v.Writes),
                                sum_writes = grp.Sum(v => v.Writes),

                                min_duration_us = grp.Min(v => v.Duration_us),
                                max_duration_us = grp.Max(v => v.Duration_us),
                                sum_duration_us = grp.Sum(v => v.Duration_us),

                                min_execution_date = grp.Min(v => v.Event_time),
                                max_execution_date = grp.Max(v => v.Event_time),

                                execution_count = grp.Count()
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
            int numRows;

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

                var Table = from t in Data.RawData.Keys
                            from v in Data.RawData[t]
                            group new
                            {
                                v.Cpu_us,
                                v.Duration_us,
                                v.Event_time,
                                v.Reads,
                                v.Writes
                            }
                            by new
                            {
                                sql_hash = t.Sql_hash,
                                application_id = t.Application_id,
                                database_id = t.Database_id,
                                host_id = t.Host_id,
                                login_id = t.Login_id
                            }
                            into grp
                            select new
                            {
                                interval_id = current_interval_id,

                                grp.Key.sql_hash,
                                grp.Key.application_id,
                                grp.Key.database_id,
                                grp.Key.host_id,
                                grp.Key.login_id,

                                avg_cpu_us = grp.Average(v => v.Cpu_us),
                                min_cpu_us = grp.Min(v => v.Cpu_us),
                                max_cpu_us = grp.Max(v => v.Cpu_us),
                                sum_cpu_us = grp.Sum(v => v.Cpu_us),

                                avg_reads = grp.Average(v => v.Reads),
                                min_reads = grp.Min(v => v.Reads),
                                max_reads = grp.Max(v => v.Reads),
                                sum_reads = grp.Sum(v => v.Reads),

                                avg_writes = grp.Average(v => v.Writes),
                                min_writes = grp.Min(v => v.Writes),
                                max_writes = grp.Max(v => v.Writes),
                                sum_writes = grp.Sum(v => v.Writes),

                                avg_duration_us = grp.Average(v => v.Duration_us),
                                min_duration_us = grp.Min(v => v.Duration_us),
                                max_duration_us = grp.Max(v => v.Duration_us),
                                sum_duration_us = grp.Sum(v => v.Duration_us),

                                execution_count = grp.Count()
                            };

                using (var reader = ObjectReader.Create(Table, "interval_id", "sql_hash", "application_id", "database_id", "host_id", "login_id", "avg_cpu_us", "min_cpu_us", "max_cpu_us", "sum_cpu_us", "avg_reads", "min_reads", "max_reads", "sum_reads", "avg_writes", "min_writes", "max_writes", "sum_writes", "avg_duration_us", "min_duration_us", "max_duration_us", "sum_duration_us", "execution_count"))
                {
                    bulkCopy.WriteToServer(reader);
                }
                numRows = Data.RawData.Sum(x => x.Value.Count);
                logger.Info($"{numRows} rows aggregated");
                numRows = Data.RawData.Count();
                logger.Info($"{numRows} rows written");
            }
        }

        private void WriteExecutionErrors(SqlConnection conn, SqlTransaction tran, int current_interval_id)
        {

            if (Data.ErrorData == null)
            {
                Data.PrepareDataTables();
            }

            lock (Data.ErrorData)
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

                    var Table = from t in Data.ErrorData.AsEnumerable()
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
                Data.ErrorData.Rows.Clear();
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

        private void WriteNormalizedQueries(SqlConnection conn, SqlTransaction tran, Dictionary<long, NormalizedQuery> values)
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
            foreach (var hash in values.Keys.ToList())
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

        public int ComputeIntervalId(DateTime intervalTime)
        {
            // interval id is the number of seconds since 01/01/2000
            return (int)intervalTime.Subtract(DateTime.MinValue.AddYears(1999)).TotalSeconds;
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

            var interval_id = ComputeIntervalId(intervalTime);

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

        public void AddAllRows(SqlConnection conn, string sql, Dictionary<string, int> d)
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
            catch (SqlException e)
            {
                logger.Trace("Unable to read saved classifiers from the analysis database: {0}", e.Message);
            }
            catch (Exception e)
            {
                logger.Error(e.Message);
                throw;
            }
        }

        /// <summary>
        /// Populates dictionaries in WorkloadData (Applications, Databases, Hosts, Logins)
        /// reading from the analysis database
        /// </summary>
        public void PopulateDictionariesFromDatabase(WorkloadData data)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));

            CreateTargetDatabase();

            using (var conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString();
                conn.Open();

                var sql = string.Format(@"SELECT * FROM [{0}].[Applications]", ConnectionInfo.SchemaName);
                AddAllRows(conn, sql, data.Applications);

                sql = string.Format(@"SELECT * FROM [{0}].[Databases]", ConnectionInfo.SchemaName);
                AddAllRows(conn, sql, data.Databases);

                sql = string.Format(@"SELECT * FROM [{0}].[Hosts]", ConnectionInfo.SchemaName);
                AddAllRows(conn, sql, data.Hosts);

                sql = string.Format(@"SELECT * FROM [{0}].[Logins]", ConnectionInfo.SchemaName);
                AddAllRows(conn, sql, data.Logins);
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

        public void CreateTargetDatabase()
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
            catch (Exception e)
            {
                logger.Warn("Unable to create the target database for the analysis", e.Message);
            }

        }
    }
}
