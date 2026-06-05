using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

using FastMember;

using NFX.DataAccess.Distributed;

using NLog;

using WorkloadTools.Util;

//--------------------------------------ATTENTION--------------------------------------
//For SQLite you need to use main.table_name to refer to permanent tables and temp.table_name to refer to temporary tables.
//If you don't use the main. prefix and there is a temporary table with the same name as the permanent table,
//SQLite will write to the temporary table instead of the permanent one without throwing any error.

namespace WorkloadTools.Consumer.Analysis
{

    internal class SqliteAnalysisDatabaseWriter : AnalysisDatabaseWriter
    {
        private SQLiteConnection _connection;
        private SQLiteTransaction _transaction;

        //Given a DataTable and the name of the table, this method fill that table with the data.
        //If it is a temporary table, tableName needs to be temp.name_of_the_table.
        //If it is a permanent table, tableName doesn't need the main. prefix because it is automaticaly added in the query.
        private void WriteToTable(DataTable dt, string tableName)
        {
            
            using (var command = new SQLiteCommand())
            {
                command.Connection = _connection;
                command.Transaction = _transaction;

                var columns = new System.Collections.Generic.List<string>();
                var schemaCommand = _connection.CreateCommand();
                schemaCommand.Transaction = _transaction;
                string[] tableNames = tableName.Split('.');
                //if is 2 that it is a temp.something table, if is 1 it is a normal table because Sqlite does't have Schemas.
                if (tableNames.Length == 2)
                {
                    schemaCommand.CommandText = $"PRAGMA [{tableNames[0]}].table_info([{tableNames[1]}]);";
                }
                else
                {
                    schemaCommand.CommandText = $"PRAGMA main.table_info([{tableName}]);";
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
                    command.CommandText = $"INSERT INTO main.[{tableName}] ({columnNames}) VALUES ({paramNames});";
                }

                var parameterObjects = new System.Collections.Generic.Dictionary<string, SQLiteParameter>();
                foreach (var col in columns)
                {
                    var param = new SQLiteParameter();
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
            logger.Debug($"{tableName} written to SQLite");
            
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public override void WriteToServer(DateTime intervalTime)
        {
            logger.Trace("Writing Workload Analysis data");

            using (_connection = new SQLiteConnection())
            {
                //CreateTargetDatabase();

                //This connection string will create the database file if it doesn't exist
                _connection.ConnectionString = ConnectionInfo.ConnectionString();
                //Here the database file will be created if it doesn't exist and the connection will be opened
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

                using (var dt = DataUtils.ToDataTable(waitRecords))
                {
                    if (dt.Rows.Count == 0)
                    {
                        return;
                    }
                    var tableName = "WaitStats";

                    WriteToTable(dt, tableName);
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

                using (var dt = DataUtils.ToDataTable(diskRecords))
                {
                    if (dt.Rows.Count == 0)
                    {
                        return;
                    }
                    var tableName = "DiskPerf";

                    WriteToTable(dt, tableName);
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

                using (var dt = DataUtils.ToDataTable(counterRecords))
                {
                    if (dt.Rows.Count == 0)
                    {
                        return;
                    }
                    var tableName = "PerformanceCounters";
                    WriteToTable(dt, tableName);
                }

                Data.PerformanceCounters.Dispose();
                Data.PerformanceCounters = null;
            }
        }

        protected override void WriteExecutionSummary()
        {
            var summaryRecords = _aggregator.AggregateExecutionSummary(Data);

            var sql = $@"
                DROP TABLE IF EXISTS temp.WorkloadSummary;

                CREATE TEMP TABLE WorkloadSummary AS
                SELECT * FROM main.WorkloadSummary WHERE 1 = 0;
            ";

            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }

            using (var reader = ObjectReader.Create(summaryRecords, "application_id", "database_id", "host_id", "login_id", "min_cpu_us", "max_cpu_us", "sum_cpu_us", "min_reads", "max_reads", "sum_reads", "min_writes", "max_writes", "sum_writes", "min_duration_us", "max_duration_us", "sum_duration_us", "min_execution_date", "max_execution_date", "execution_count"))
            {
                var dt = new DataTable();
                dt.Load(reader);
                var tableName = "temp.WorkloadSummary";
                WriteToTable(dt, tableName);
            }

            var affectedRows = 0;

            sql = $@"
                UPDATE main.WorkloadSummary AS WS
                SET min_cpu_us = CASE WHEN T.min_cpu_us < WS.min_cpu_us THEN T.min_cpu_us ELSE WS.min_cpu_us END,
                    max_cpu_us = CASE WHEN T.max_cpu_us > WS.max_cpu_us THEN T.max_cpu_us ELSE WS.max_cpu_us END,
                    sum_cpu_us = WS.sum_cpu_us + T.sum_cpu_us,
                    min_reads  = CASE WHEN T.min_reads < WS.min_reads THEN T.min_reads ELSE WS.min_reads END,
                    max_reads  = CASE WHEN T.max_reads > WS.max_reads THEN T.max_reads ELSE WS.max_reads END,
                    sum_reads  = WS.sum_reads + T.sum_reads,
                    min_writes = CASE WHEN T.min_writes < WS.min_writes THEN T.min_writes ELSE WS.min_writes END,
                    max_writes = CASE WHEN T.max_writes > WS.max_writes THEN T.max_writes ELSE WS.max_writes END,
                    sum_writes = WS.sum_writes + T.sum_writes,
                    min_duration_us = CASE WHEN T.min_duration_us < WS.min_duration_us THEN T.min_duration_us ELSE WS.min_duration_us END,
                    max_duration_us = CASE WHEN T.max_duration_us > WS.max_duration_us THEN T.max_duration_us ELSE WS.max_duration_us END,
                    sum_duration_us = WS.sum_duration_us + T.sum_duration_us,
                    min_execution_date = CASE WHEN T.min_execution_date < WS.min_execution_date THEN T.min_execution_date ELSE WS.min_execution_date END,
                    max_execution_date = CASE WHEN T.max_execution_date > WS.max_execution_date THEN T.max_execution_date ELSE WS.max_execution_date END,
                    execution_count = WS.execution_count + T.execution_count
                FROM temp.WorkloadSummary AS T
                WHERE T.application_id   = WS.application_id
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
                INSERT INTO main.WorkloadSummary 
                SELECT * 
                FROM temp.WorkloadSummary AS T
                WHERE NOT EXISTS (
                    SELECT *
                    FROM main.WorkloadSummary AS WS
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

            using (var reader = ObjectReader.Create(detailRecords, "interval_id", "sql_hash", "application_id", "database_id", "host_id", "login_id", "avg_cpu_us", "min_cpu_us", "max_cpu_us", "sum_cpu_us", "avg_reads", "min_reads", "max_reads", "sum_reads", "avg_writes", "min_writes", "max_writes", "sum_writes", "avg_duration_us", "min_duration_us", "max_duration_us", "sum_duration_us", "execution_count"))
            {
                var dataTable = new DataTable();
                dataTable.Load(reader);
                var tableName = "WorkloadDetails";
                WriteToTable(dataTable, tableName);
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

            dynamic errorRecords = _aggregator.AggregateErrors(Data.ErrorData, current_interval_id);

            lock (Data.ErrorData)
            {

                using (var dt = DataUtils.ToDataTable(errorRecords))
                {
                    if (dt.Rows.Count == 0)
                    {
                        return;
                    }
                    var tableName = "Errors";

                    WriteToTable(dt, tableName);
                }

                Data.DiskPerfData.Dispose();
                Data.DiskPerfData = null;
            }
        }

        protected override void WriteDictionary(Dictionary<string, int> values, string name)
        {
            var sql = @"
                DROP TABLE IF EXISTS temp.[{0}];

                CREATE TEMP TABLE [{0}] AS
                SELECT * FROM main.[{0}] WHERE 0=1;
            ";
            sql = string.Format(sql, name);

            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }

            var table = new DataTable();
            _ = table.Columns.Add($"{name.Substring(0, name.Length - 1)}_id", typeof(int));
            _ = table.Columns.Add($"{name.Substring(0, name.Length - 1)}_name", typeof(string));
            foreach (var item in values)
            {
                _ = table.Rows.Add(item.Value, item.Key);
            }
            WriteToTable(table, "temp." + name);

            sql = @"
                INSERT INTO main.[{0}s]
                SELECT *
                FROM temp.[{0}s] AS src
                WHERE NOT EXISTS (
                    SELECT *
                    FROM main.[{0}s] AS dst 
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
                SELECT * FROM main.[{0}] WHERE 0=1;
            ";
            sql = string.Format(sql, tableName);

            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }

            WriteToTable(DataUtils.ToDataTable(values.Where(t => t.Value != null).Select(t => new { sql_hash = t.Value.Hash, normalized_text = t.Value.NormalizedText, example_text = t.Value.ExampleText })), "temp." + tableName);

            sql = @"
                INSERT INTO main.[{0}]
                SELECT *
                FROM temp.{0} AS src
                WHERE NOT EXISTS (
                    SELECT *
                    FROM main.[{0}] AS dst 
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
            var sql_Update = @"
                UPDATE main.[Intervals]
                SET  end_time = @end_time
                    ,duration_minutes = @duration_minutes
                WHERE interval_id = @interval_id;
            ";
            var sql_Insert = @"
                INSERT INTO main.[Intervals] (interval_id, end_time, duration_minutes) 
                VALUES (@interval_id, @end_time, @duration_minutes);
            ";

            var interval_id = ComputeIntervalId(intervalTime);

            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;
                cmd.CommandText = sql_Update;
                _ = cmd.Parameters.AddWithValue("@interval_id", interval_id);
                _ = cmd.Parameters.AddWithValue("@end_time", intervalTime);
                _ = cmd.Parameters.AddWithValue("@duration_minutes", Interval);
                var row_count = cmd.ExecuteNonQuery();

                if (row_count == 0)
                {
                    cmd.CommandText = sql_Insert;
                    _ = cmd.Parameters.AddWithValue("@interval_id", interval_id);
                    _ = cmd.Parameters.AddWithValue("@end_time", intervalTime);
                    _ = cmd.Parameters.AddWithValue("@duration_minutes", Interval);
                    _ = cmd.ExecuteNonQuery();
                }
            }

            if (!FirstIntervalWritten)
            {
                using (var cmd = _connection.CreateCommand())
                {
                    cmd.Transaction = _transaction;
                    cmd.CommandText = sql_Update;
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

            var sql = File.ReadAllText(WorkloadController.BaseLocation + "\\Consumer\\Analysis\\SqliteDatabaseSchema.sql");

            using (var conn = new SQLiteConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString();
                conn.Open();

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
                using (var connection = new SQLiteConnection())
                {
                    connection.ConnectionString = ConnectionInfo.ConnectionString();
                    connection.Open();
                    logger.Info($"Database {connection.DataSource} created successfully.");
                }
            }
            catch(SQLiteException e)
            {
                logger.Error("Unable to create the target database for the analysis", e.Message);
            }
            catch(Exception e)
            {
                logger.Error(e.Message);
                throw;
            }
        }

        protected override void PopulateDictionariesFromDatabaseInternal(WorkloadData data)
        {
            using (var conn = new SQLiteConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString();
                conn.Open();

                var sql = @"SELECT * FROM main.[Applications]";
                AddAllRowsInternal(conn, sql, data.Applications);

                sql = @"SELECT * FROM main.[Databases]";
                AddAllRowsInternal(conn, sql, data.Databases);

                sql = @"SELECT * FROM main.[Hosts]";
                AddAllRowsInternal(conn, sql, data.Hosts);

                sql = @"SELECT * FROM main.[Logins]";
                AddAllRowsInternal(conn, sql, data.Logins);
            }
        }

        protected override void AddAllRowsInternal(DbConnection conn, string sql, Dictionary<string, int> d)
        {
            try
            {
                using (var adapter = new SQLiteDataAdapter(sql, (SQLiteConnection)conn))
                {
                    using (var ds = new DataSet())
                    {
                        _ = adapter.Fill(ds);
                        var dt = ds.Tables[0];
                        foreach (DataRow dr in dt.Rows)
                        {
                            d.Add((string)dr[1], Convert.ToInt32(dr[0]));
                        }
                    }
                }
            }
            catch (SQLiteException e)
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