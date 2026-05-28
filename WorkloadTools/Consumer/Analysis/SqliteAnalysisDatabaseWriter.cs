using System;
using System.IO;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

using FastMember;

using Microsoft.Data.Sqlite;

using NFX.DataAccess.Distributed;

using NLog;

using WorkloadTools.Util;
using System.Data.SQLite;

namespace WorkloadTools.Consumer.Analysis
{
    internal class SqliteAnalysisDatabaseWriter : AnalysisDatabaseWriter
    {
        //TODO: We need to make this connection string like the other one for Sql Server and also add other parameters.
        private readonly string connectionString = new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder()
        {
            DataSource = "analysis.db",
            //this is on default
            Mode = Microsoft.Data.Sqlite.SqliteOpenMode.ReadWriteCreate
        }.ToString();

        private Microsoft.Data.Sqlite.SqliteConnection _connection;
        private Microsoft.Data.Sqlite.SqliteTransaction _transaction;

        /*
        private void WriteToDatabase(IDataReader reader, string tableName)
        {
            while (reader.Read())
            {
                foreach (var col in columns)
                {
                    try
                    {
                        int ordinal = reader.GetOrdinal(col);
                        parameterObjects[col].Value = reader.GetValue(ordinal) ?? DBNull.Value;
                    }
                    catch (IndexOutOfRangeException)
                    {
                        parameterObjects[col].Value = DBNull.Value;
                    }
                }
                _ = command.ExecuteNonQuery();
            }
        }
        */

        private void WriteToDatabase(DataTable dt, string tableName)
        {
            using (var transaction = _transaction)
            {
                using (var command = new Microsoft.Data.Sqlite.SqliteCommand())
                {
                    command.Connection = _connection;
                    command.Transaction = transaction;

                    var columns = new System.Collections.Generic.List<string>();
                    var schemaCommand = _connection.CreateCommand();
                    schemaCommand.Transaction = transaction;
                    string[] tableNames = tableName.Split('.');
                    //if is 2 that it is a temp.something table, if is 1 it is a normal table, SQLite does not support schemas so we ignore the first part if it exists.
                    if (tableNames.Length == 2)
                    {
                        schemaCommand.CommandText = $"PRAGMA [{tableNames[0]}].table_info([{tableNames[1]}]);";
                    }
                    else
                    {
                        schemaCommand.CommandText = $"PRAGMA table_info([{tableName}]);";
                    }

                    using (var reader = schemaCommand.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            columns.Add(reader.GetString(1));
                        }
                    }

                    var columnNames = string.Join(", ", columns.ConvertAll(c => $"[{c}]"));
                    var paramNames = string.Join(", ", columns.ConvertAll(c => $"${c}"));
                    if (tableNames.Length == 2)
                    {
                        command.CommandText = $"INSERT INTO [{tableNames[0]}].[{tableNames[1]}] ({columnNames}) VALUES ({paramNames});";
                    }
                    else
                    {
                        command.CommandText = $"INSERT INTO [{tableName}] ({columnNames}) VALUES ({paramNames});";
                    }

                    var parameterObjects = new System.Collections.Generic.Dictionary<string, Microsoft.Data.Sqlite.SqliteParameter>();
                    foreach (var col in columns)
                    {
                        var param = new Microsoft.Data.Sqlite.SqliteParameter();
                        param.ParameterName = $"${col}";
                        _ = command.Parameters.Add(param);
                        parameterObjects[col] = param;
                    }

                    foreach (System.Data.DataRow row in dt.Rows)
                    {
                        foreach (var col in columns)
                        {
                            if (dt.Columns.Contains(col))
                            {
                                parameterObjects[col].Value = row[col] ?? DBNull.Value;
                            }
                            else
                            {
                                parameterObjects[col].Value = DBNull.Value;
                            }
                        }
                        _ = command.ExecuteNonQuery();
                    }
                }
                logger.Info($"{tableName} written to SQLite");
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public override void WriteToServer(DateTime intervalTime)
        {
            logger.Trace("Writing Workload Analysis data");

            using (_connection = new Microsoft.Data.Sqlite.SqliteConnection())
            {
                _connection.ConnectionString = connectionString;
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

            var waitRecords = _aggregator.AggregateWaitStats(Data.WaitsData, current_interval_id);

            lock (Data.WaitsData)
            {

                using (var dt = DataUtils.ToDataTable(waitRecords))
                {
                    if (dt.Rows.Count == 0) return;

                    var tableName = "WaitStats";

                    WriteToDatabase(dt, tableName);
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

            var diskRecords = _aggregator.AggregateDiskPerf(Data.DiskPerfData, current_interval_id);

            lock (Data.DiskPerfData)
            {

                using (var dt = DataUtils.ToDataTable(diskRecords))
                {
                    if (dt.Rows.Count == 0) return;

                    var tableName = "DiskPerf";

                    WriteToDatabase(dt, tableName);
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

            var counterRecords = _aggregator.AggregatePerformanceCounters(Data.PerformanceCounters, current_interval_id);

            lock (Data.PerformanceCounters)
            {

                using (var dt = DataUtils.ToDataTable(counterRecords))
                {
                    if (dt.Rows.Count == 0) return;

                    var tableName = "PerformanceCounters";

                    WriteToDatabase(dt, tableName);
                }

                Data.DiskPerfData.Dispose();
                Data.DiskPerfData = null;
            }
        }

        protected override void WriteExecutionSummary()
        {
            var summaryRecords = _aggregator.AggregateExecutionSummary(Data);

            var sql = $@"
                DROP TABLE IF EXISTS temp.WorkloadSummary;

                CREATE TEMP TABLE WorkloadSummary AS
                SELECT * FROM WorkloadSummary WHERE 1 = 0;
            ";

            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }

            using (var reader = ObjectReader.Create(summaryRecords, "ApplicationId", "DatabaseId", "HostId", "LoginId", "MinCpuUs", "MaxCpuUs", "SumCpuUs", "MinReads", "MaxReads", "SumReads", "MinWrites", "MaxWrites", "SumWrites", "MinDurationUs", "MaxDurationUs", "SumDurationUs", "MinExecutionDate", "MaxExecutionDate", "ExecutionCount"))
            {
                var dt = new DataTable();
                dt.Load(reader);
                var tableName = "temp.WorkloadSummary";
                WriteToDatabase(dt, tableName);
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
                FROM WorkloadSummary AS WS
                INNER JOIN temp.WorkloadSummary AS T
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
                INSERT INTO WorkloadSummary 
                SELECT * 
                FROM temp.WorkloadSummary AS T
                WHERE NOT EXISTS (
                    SELECT *
                    FROM WorkloadSummary AS WS
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


        //TODO: THIS METHOD NEED TO BE OPTIMIZED FOR SQLITE. WE NEED TO CREATE A WriteToDatabase FOR AN IDataReader.
        protected override void WriteExecutionDetails(int current_interval_id)
        {
            var detailRecords = _aggregator.AggregateExecutionDetails(Data, current_interval_id);

            int numRows;

            using (var reader = ObjectReader.Create(detailRecords, "IntervalId", "SqlHash", "ApplicationId", "DatabaseId", "HostId", "LoginId", "AvgCpuUs", "MinCpuUs", "MaxCpuUs", "SumCpuUs", "AvgReads", "MinReads", "MaxReads", "SumReads", "AvgWrites", "MinWrites", "MaxWrites", "SumWrites", "AvgDurationUs", "MinDurationUs", "MaxDurationUs", "SumDurationUs", "ExecutionCount"))
            {
                var tableName = "WorkloadDetails";
                WriteToDatabase(reader, tableName);
            }
            numRows = Data.RawData.Sum(x => x.Value.Count);
            logger.Info($"{numRows} rows aggregated");
            numRows = Data.RawData.Count();
            logger.Info($"{numRows} rows written");
        }

        protected override void WriteExecutionErrors(int current_interval_id)
        {
            if (Data.ErrorData == null)
            {
                return;
            }

            var errorRecords = _aggregator.AggregateErrors(Data.ErrorData, current_interval_id);

            lock (Data.ErrorData)
            {

                using (var dt = DataUtils.ToDataTable(errorRecords))
                {
                    if (dt.Rows.Count == 0) return;

                    var tableName = "Error";

                    WriteToDatabase(dt, tableName);
                }

                Data.DiskPerfData.Dispose();
                Data.DiskPerfData = null;
            }
        }

        protected override void WriteDictionary(Dictionary<string, int> values, string name)
        {
            var sql = @"
                CREATE TEMP TABLE [{0}] AS
                SELECT * FROM [{0}] WHERE 0=1;
            ";
            sql = string.Format(sql, name);

            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }

            WriteToDatabase(DataUtils.ToDataTable(from t in values select new { t.Value, t.Key }), "temp."+name);

            //REMEMBER TO TELL BOSS THAT THERE ARE S's THAT ARE ELIMINATED AND REPLACED WHIT S's :D
            sql = @"
                INSERT INTO [{0}s]
                SELECT *
                FROM temp.[{0}s] AS src
                WHERE NOT EXISTS (
                    SELECT *
                    FROM [{0}s] AS dst 
                    WHERE dst.[{0}_id] = src.[{0}_id]
                );
            ";
            sql = string.Format(sql, name.Substring(0, name.Length - 1));

            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }
        }

        protected override void WriteNormalizedQueries(Dictionary<long, NormalizedQuery> values)
        {
            var tableName = "NormalizedQueries";

            var sql = @"
                CREATE TEMP TABLE [{0}] AS
                SELECT * FROM [{0}] WHERE 0=1;
            ";
            sql = string.Format(sql, tableName);

            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }

            WriteToDatabase(DataUtils.ToDataTable(from t in values where t.Value != null select new { t.Value.Hash, t.Value.NormalizedText, t.Value.ExampleText }), "temp."+tableName);

            sql = @"
                INSERT INTO [{0}]
                SELECT *
                FROM temp.{0} AS src
                WHERE NOT EXISTS (
                    SELECT *
                    FROM [{0}] AS dst 
                    WHERE dst.[sql_hash] = src.[sql_hash]
                );
            ";
            sql = string.Format(sql, tableName);

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
            //TODO: I DO NOT KNOW HOW TO DO THE IF FOR SQLITE BECAUSE IT DOES NOT EXIST.
            var sql = @"
                UPDATE [Intervals]
                SET  end_time = @end_time
                    ,duration_minutes = @duration_minutes
                WHERE interval_id = @interval_id;

                IF @@ROWCOUNT = 0
                    INSERT INTO [Intervals] (interval_id, end_time, duration_minutes) 
                    VALUES (@interval_id, @end_time, @duration_minutes); 
            ";
            //sql = string.Format(sql);

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

        //TODO: We need to create a connectrion info for SQLite.
        protected override void CreateTargetTables()
        {
            CreateTargetDatabase();

            var sql = File.ReadAllText(WorkloadController.BaseLocation + "\\Consumer\\Analysis\\SqliteDatabaseSchema.sql");

            sql = sql.Replace("{DatabaseName}", ConnectionInfo.DatabaseName);

            using (var conn = new Microsoft.Data.Sqlite.SqliteConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();
                conn.ChangeDatabase(ConnectionInfo.DatabaseName);

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = sql;
                    _ = cmd.ExecuteNonQuery();
                }

                //TODO: From here I have no idea of what to do
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
            throw new NotImplementedException("SQLite support for analysis database writing is not yet implemented");
        }

        //TODO: This method need a new ConnectionInfo for SQLite for the connection string and other info.
        protected override void PopulateDictionariesFromDatabaseInternal(WorkloadData data)
        {
            using (var conn = new Microsoft.Data.Sqlite.SqliteConnection())
            {
                conn.ConnectionString = connectionString;
                conn.Open();

                var sql = @"SELECT * FROM [Applications]";
                AddAllRowsInternal(conn, sql, data.Applications);

                sql = @"SELECT * FROM [Databases]";
                AddAllRowsInternal(conn, sql, data.Databases);

                sql = @"SELECT * FROM [Hosts]";
                AddAllRowsInternal(conn, sql, data.Hosts);

                sql = @"SELECT * FROM [Logins]";
                AddAllRowsInternal(conn, sql, data.Logins);
            }
        }

        //TODO: we need a method to have different parameters for only this method in this class.
        protected override void AddAllRowsInternal(Microsoft.Data.Sqlite.SqliteConnection conn, string sql, Dictionary<string, int> d)
        {
            try
            {
                using (var cmd = new Microsoft.Data.Sqlite.SqliteCommand(commandText: sql, connection: conn))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            d.Add((string)reader[1], (int)reader[0]);
                        }
                    }
                }
            }
            catch (Microsoft.Data.Sqlite.SqliteException e)
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
