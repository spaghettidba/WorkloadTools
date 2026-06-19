using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

using FastMember;

using NLog;

using WorkloadTools.Util;

namespace WorkloadTools.Consumer.Analysis
{
    internal class SqlServerAnalysisDatabaseWriter : AnalysisDatabaseWriter
    {
        private SqlConnection _connection;
        private SqlTransaction _transaction;

        protected override void WriteToTable(DataTable dt, string tableName)
        {
            logger.Warn("WHY ARE YOU HERE?");
            return;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public override void WriteToServer(DateTime intervalTime)
        {
            logger.Trace("Writing Workload Analysis data");

            using (_connection = new SqlConnection())
            {
                _connection.ConnectionString = ConnectionInfo.ConnectionString();
                _connection.Open();

                if (!TargetTableCreated)
                {
                    CreateTargetTables();
                    TargetTableCreated = true;
                }

                _transaction = _connection.BeginTransaction();

                try
                {
                    var current_interval_id = 0;
                    if (WriteDetail)
                    {
                        current_interval_id = CreateInterval(intervalTime);
                    }

                    WriteDictionary(Data.Applications, "applications");
                    WriteDictionary(Data.Databases, "databases");
                    WriteDictionary(Data.Hosts, "hosts");
                    WriteDictionary(Data.Logins, "logins");

                    lock (Data)
                    {
                        if (WriteSummary)
                        {
                            WriteExecutionSummary();
                        }

                        if (WriteDetail)
                        {
                            WriteExecutionDetails(current_interval_id);
                        }

                        Data.ClearRawData();
                    }

                    if (WriteDetail)
                    {
                        WriteNormalizedQueries(Data.NormalizedQueries);
                        WriteExecutionErrors(current_interval_id);
                        WritePerformanceCounters(current_interval_id);
                        WriteWaitsData(current_interval_id);
                        WriteDiskPerf(current_interval_id);
                    }

                    _transaction.Commit();
                    if (WriteDetail)
                    {
                        LastWrittenIntervalId = current_interval_id;
                    }
                }
                catch (Exception)
                {
                    _transaction.Rollback();
                    throw;
                }
            }
        }

        protected override void WriteWaitsData(int current_interval_id)
        {
            if (Data.WaitsData == null)
            {
                return;
            }

            dynamic waitRecords = _aggregator.AggregateWaitStats(Data.WaitsData, current_interval_id);

            lock (Data.WaitsData)
            {
                using (var bulkCopy = new System.Data.SqlClient.SqlBulkCopy(_connection,
                                                SqlBulkCopyOptions.KeepIdentity |
                                                SqlBulkCopyOptions.FireTriggers |
                                                SqlBulkCopyOptions.CheckConstraints |
                                                SqlBulkCopyOptions.TableLock,
                                                _transaction))
                {
                    bulkCopy.DestinationTableName = "[" + ConnectionInfo.SchemaName + "].[WaitStats]";
                    bulkCopy.BatchSize = 1000;
                    bulkCopy.BulkCopyTimeout = 300;

                    using (var dt = DataUtils.ToDataTable(waitRecords))
                    {
                        bulkCopy.WriteToServer(dt);
                    }

                    logger.Info("Wait stats written");
                }
                Data.WaitsData.Dispose();
                Data.WaitsData = null;
            }
        }

        protected override void WriteDiskPerf(int current_interval_id)
        {
            if (Data.DiskPerfData == null)
            {
                return;
            }

            dynamic diskRecords = _aggregator.AggregateDiskPerf(Data.DiskPerfData, current_interval_id);

            lock (Data.DiskPerfData)
            {
                using (var bulkCopy = new System.Data.SqlClient.SqlBulkCopy(_connection,
                                                SqlBulkCopyOptions.KeepIdentity |
                                                SqlBulkCopyOptions.FireTriggers |
                                                SqlBulkCopyOptions.CheckConstraints |
                                                SqlBulkCopyOptions.TableLock,
                                                _transaction))
                {
                    bulkCopy.DestinationTableName = "[" + ConnectionInfo.SchemaName + "].[DiskPerf]";
                    bulkCopy.BatchSize = 1000;
                    bulkCopy.BulkCopyTimeout = 300;

                    using (var dt = DataUtils.ToDataTable(diskRecords))
                    {
                        bulkCopy.WriteToServer(dt);
                    }

                    logger.Info("Disk perf written");
                }
                Data.DiskPerfData.Dispose();
                Data.DiskPerfData = null;
            }
        }

        protected override void WritePerformanceCounters(int current_interval_id)
        {
            if (Data.PerformanceCounters == null)
            {
                return;
            }

            dynamic counterRecords = _aggregator.AggregatePerformanceCounters(Data.PerformanceCounters, current_interval_id);

            lock (Data.PerformanceCounters)
            {
                using (var bulkCopy = new System.Data.SqlClient.SqlBulkCopy(_connection,
                                                SqlBulkCopyOptions.KeepIdentity |
                                                SqlBulkCopyOptions.FireTriggers |
                                                SqlBulkCopyOptions.CheckConstraints |
                                                SqlBulkCopyOptions.TableLock,
                                                _transaction))
                {
                    bulkCopy.DestinationTableName = "[" + ConnectionInfo.SchemaName + "].[PerformanceCounters]";
                    bulkCopy.BatchSize = 1000;
                    bulkCopy.BulkCopyTimeout = 300;

                    using (var dt = DataUtils.ToDataTable(counterRecords))
                    {
                        bulkCopy.WriteToServer(dt);
                    }
                    logger.Info("Performance counters written");
                }
                Data.PerformanceCounters.Dispose();
                Data.PerformanceCounters = null;
            }
        }

        protected override void WriteExecutionSummary()
        {
            dynamic summaryRecords = _aggregator.AggregateExecutionSummary(Data);

            var sql = $@"
                IF OBJECT_ID('tempdb..#WorkloadSummary') IS NOT NULL 
                    DROP TABLE #WorkloadSummary;

                SELECT TOP(0) * INTO #WorkloadSummary FROM [{ConnectionInfo.SchemaName}].WorkloadSummary;
            ";
            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }

            using (var bulkCopy = new System.Data.SqlClient.SqlBulkCopy(_connection,
                                            SqlBulkCopyOptions.KeepIdentity |
                                            SqlBulkCopyOptions.FireTriggers |
                                            SqlBulkCopyOptions.CheckConstraints |
                                            SqlBulkCopyOptions.TableLock,
                                            _transaction))
            {
                bulkCopy.DestinationTableName = "#WorkloadSummary";
                bulkCopy.BatchSize = 1000;
                bulkCopy.BulkCopyTimeout = 300;

                using (var reader = ObjectReader.Create(summaryRecords, "application_id", "database_id", "host_id", "login_id", "min_cpu_us", "max_cpu_us", "sum_cpu_us", "min_reads", "max_reads", "sum_reads", "min_writes", "max_writes", "sum_writes", "min_duration_us", "max_duration_us", "sum_duration_us", "min_execution_date", "max_execution_date", "execution_count"))
                {
                    bulkCopy.WriteToServer(reader);
                }
            }

            var affectedRows = 0;

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
            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;
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
            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;
                cmd.CommandText = sql;
                affectedRows += cmd.ExecuteNonQuery();
            }

            logger.Info($"Summary info written ({affectedRows} rows)");
        }

        protected override void WriteExecutionDetails(int current_interval_id)
        {
            dynamic detailRecords = _aggregator.AggregateExecutionDetails(Data, current_interval_id);

            int numRows;

            using (var bulkCopy = new System.Data.SqlClient.SqlBulkCopy(_connection,
                                            SqlBulkCopyOptions.KeepIdentity |
                                            SqlBulkCopyOptions.FireTriggers |
                                            SqlBulkCopyOptions.CheckConstraints |
                                            SqlBulkCopyOptions.TableLock,
                                            _transaction))
            {
                bulkCopy.DestinationTableName = "[" + ConnectionInfo.SchemaName + "].[WorkloadDetails]";
                bulkCopy.BatchSize = 1000;
                bulkCopy.BulkCopyTimeout = 300;

                using (var reader = ObjectReader.Create(detailRecords, "interval_id", "sql_hash", "application_id", "database_id", "host_id", "login_id", "avg_cpu_us", "min_cpu_us", "max_cpu_us", "sum_cpu_us", "avg_reads", "min_reads", "max_reads", "sum_reads", "avg_writes", "min_writes", "max_writes", "sum_writes", "avg_duration_us", "min_duration_us", "max_duration_us", "sum_duration_us", "execution_count"))
                {
                    bulkCopy.WriteToServer(reader);
                }
                numRows = Data.RawData.Sum(x => x.Value.Count);
                logger.Info($"{numRows} rows aggregated");
                numRows = Data.RawData.Count();
                logger.Info($"{numRows} rows written");
            }
        }

        protected override void WriteExecutionErrors(int current_interval_id)
        {
            if (Data.ErrorData == null)
            {
                Data.PrepareDataTables();
            }

            dynamic errorRecords = _aggregator.AggregateErrors(Data.ErrorData, current_interval_id);

            lock (Data.ErrorData)
            {
                using (var bulkCopy = new System.Data.SqlClient.SqlBulkCopy(_connection,
                                                SqlBulkCopyOptions.KeepIdentity |
                                                SqlBulkCopyOptions.FireTriggers |
                                                SqlBulkCopyOptions.CheckConstraints |
                                                SqlBulkCopyOptions.TableLock,
                                                _transaction))
                {
                    bulkCopy.DestinationTableName = "[" + ConnectionInfo.SchemaName + "].[Errors]";
                    bulkCopy.BatchSize = 1000;
                    bulkCopy.BulkCopyTimeout = 300;

                    bulkCopy.WriteToServer(DataUtils.ToDataTable(errorRecords));
                }
                Data.ErrorData.Rows.Clear();
            }
        }

        protected override void WriteDictionary(Dictionary<string, int> values, string name)
        {
            var sql = @"
                SELECT TOP(0) *
                INTO #{0}
                FROM [{1}].[{0}];
            ";
            sql = string.Format(sql, name, ConnectionInfo.SchemaName);

            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }

            using (var bulkCopy = new System.Data.SqlClient.SqlBulkCopy(_connection,
                                                                SqlBulkCopyOptions.KeepIdentity |
                                                                SqlBulkCopyOptions.FireTriggers |
                                                                SqlBulkCopyOptions.CheckConstraints |
                                                                SqlBulkCopyOptions.TableLock,
                                                                _transaction))
            {
                bulkCopy.DestinationTableName = "#" + name;
                bulkCopy.BatchSize = 1000;
                bulkCopy.BulkCopyTimeout = 300;
                bulkCopy.WriteToServer(DataUtils.ToDataTable(from t in values select new { t.Value, t.Key }));
            }

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

            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }
        }

        protected override void WriteNormalizedQueries(Dictionary<long, NormalizedQuery> values)
        {
            var sql = @"
                SELECT TOP(0) *
                INTO #NormalizedQueries
                FROM [{0}].[NormalizedQueries];
            ";
            sql = string.Format(sql, ConnectionInfo.SchemaName);

            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }

            using (var bulkCopy = new System.Data.SqlClient.SqlBulkCopy(_connection,
                                                                SqlBulkCopyOptions.KeepIdentity |
                                                                SqlBulkCopyOptions.FireTriggers |
                                                                SqlBulkCopyOptions.CheckConstraints |
                                                                SqlBulkCopyOptions.TableLock,
                                                                _transaction))
            {
                bulkCopy.DestinationTableName = "#NormalizedQueries";
                bulkCopy.BatchSize = 1000;
                bulkCopy.BulkCopyTimeout = 300;
                bulkCopy.WriteToServer(DataUtils.ToDataTable(from t in values where t.Value != null select new { t.Value.Hash, t.Value.NormalizedText, t.Value.ExampleText }));
            }

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

            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }

            foreach (var hash in values.Keys.ToList())
            {
                values[hash] = null;
            }
            _ = Task.Factory.StartNew(() => InvokeGC());
        }

        protected override int CreateInterval(DateTime intervalTime)
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

            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;
                cmd.CommandText = sql;
                _ = cmd.Parameters.AddWithValue("@interval_id", interval_id);
                _ = cmd.Parameters.AddWithValue("@end_time", intervalTime);
                _ = cmd.Parameters.AddWithValue("@duration_minutes", Interval);
                _ = cmd.ExecuteNonQuery();
            }

            if (!FirstIntervalWritten)
            {
                using (var cmd = _connection.CreateCommand())
                {
                    cmd.Transaction = _transaction;
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

        protected override void CreateTargetTables()
        {
            CreateTargetDatabase();

            var sql = File.ReadAllText(WorkloadController.BaseLocation + "\\Consumer\\Analysis\\SqlServerDatabaseSchema.sql");

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

        protected override void CreateTargetDatabase()
        {
            try
            {
                var databaseName = ConnectionInfo.DatabaseName;
                using (var conn = new SqlConnection())
                {
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

        protected override void PopulateDictionariesFromDatabaseInternal(WorkloadData data)
        {
            using (var conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString();
                conn.Open();

                var sql = string.Format(@"SELECT * FROM [{0}].[Applications]", ConnectionInfo.SchemaName);
                AddAllRowsInternal(conn, sql, data.Applications);

                sql = string.Format(@"SELECT * FROM [{0}].[Databases]", ConnectionInfo.SchemaName);
                AddAllRowsInternal(conn, sql, data.Databases);

                sql = string.Format(@"SELECT * FROM [{0}].[Hosts]", ConnectionInfo.SchemaName);
                AddAllRowsInternal(conn, sql, data.Hosts);

                sql = string.Format(@"SELECT * FROM [{0}].[Logins]", ConnectionInfo.SchemaName);
                AddAllRowsInternal(conn, sql, data.Logins);
            }
        }

        protected override void AddAllRowsInternal(DbConnection conn, string sql, Dictionary<string, int> d)
        {
            try
            {
                using (var adapter = new SqlDataAdapter(sql, (SqlConnection)conn))
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
    }
}
