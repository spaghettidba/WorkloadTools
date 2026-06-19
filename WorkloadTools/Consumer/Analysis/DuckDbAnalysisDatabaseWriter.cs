using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Runtime.CompilerServices;

using DuckDB.NET.Data;

using FastMember;

using NLog;

using WorkloadTools.Util;

namespace WorkloadTools.Consumer.Analysis
{
    internal class DuckDBAnalysisDatabaseWriter : SqliteAnalysisDatabaseWriter
    {
        private DuckDBConnection _connection;
        private DuckDBTransaction _transaction;

        //Given a DataTable and the name of the table, this method fill that table with the data.
        //If it is a temporary table, tableName needs to be temp.name_of_the_table.
        //If it is a permanent table, tableName doesn't need the ""{ConnectionInfo.SchemaName}"". prefix because it is automaticaly added in the query.
        protected override void WriteToTable(DataTable dt, string tableName)
        {

            using (var command = new DuckDBCommand())
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
                    schemaCommand.CommandText = $@"PRAGMA table_info(""{tableNames[0]}"".""{tableNames[1]}"");";
                }
                else
                {
                    schemaCommand.CommandText = $@"PRAGMA table_info(""{ConnectionInfo.SchemaName}"".""{tableName}"");";
                }

                using (var reader = schemaCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        columns.Add(reader.GetString(1));
                        //logger.Info(reader.GetString(2)); // type column
                    }
                }

                var columnNames = string.Join(", ", columns.ConvertAll(c => $@"""{c}"""));
                var paramNames = string.Join(", ", columns.ConvertAll(c => $"?"));
                if (tableNames.Length == 2)
                {
                    command.CommandText = $@"INSERT INTO ""{tableNames[0]}"".""{tableNames[1]}"" ({columnNames}) VALUES ({paramNames});";
                }
                else
                {
                    command.CommandText = $@"INSERT INTO ""{ConnectionInfo.SchemaName}"".""{tableName}"" ({columnNames}) VALUES ({paramNames});";
                }

                var parameterObjects = new System.Collections.Generic.Dictionary<string, DuckDBParameter>();
                foreach (var col in columns)
                {
                    var param = new DuckDBParameter
                    {
                        DbType = GetDbType(dt.Columns[col].DataType)
                    };
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
            logger.Debug($"{tableName} written to DuckDB");
        }

        private static DbType GetDbType(Type type)
        {
            if (type == typeof(string))
                return DbType.String;
            if (type == typeof(int))
                return DbType.Int32;
            if (type == typeof(long))
                return DbType.Int64;
            if (type == typeof(decimal))
                return DbType.Decimal;
            if (type == typeof(double))
                return DbType.Double;
            if (type == typeof(DateTime))
                return DbType.DateTime;
            if (type == typeof(bool))
                return DbType.Boolean;

            return DbType.Object;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public override void WriteToServer(DateTime intervalTime)
        {
            logger.Trace("Writing Workload Analysis data");

            using (_connection = new DuckDBConnection())
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

        protected override void WriteExecutionSummary()
        {
            var summaryRecords = _aggregator.AggregateExecutionSummary(Data);

            var sql = $@"
                DROP TABLE IF EXISTS temp.WorkloadSummary;

                CREATE TEMP TABLE WorkloadSummary AS
                SELECT * FROM ""{ConnectionInfo.SchemaName}"".WorkloadSummary WHERE 1 = 0;
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
                UPDATE ""{ConnectionInfo.SchemaName}"".WorkloadSummary AS WS
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
                INSERT INTO ""{ConnectionInfo.SchemaName}"".WorkloadSummary 
                SELECT * 
                FROM temp.WorkloadSummary AS T
                WHERE NOT EXISTS (
                    SELECT *
                    FROM ""{ConnectionInfo.SchemaName}"".WorkloadSummary AS WS
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

        protected override void WriteDictionary(Dictionary<string, int> values, string name)
        {
            var sql = @"
                DROP TABLE IF EXISTS temp.""{0}"";

                CREATE TEMP TABLE ""{0}"" AS
                SELECT * FROM ""{1}"".""{0}"" WHERE 0=1;
            ";
            sql = string.Format(sql, name, ConnectionInfo.SchemaName);

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
                INSERT INTO ""{1}"".""{0}s""
                SELECT *
                FROM temp.""{0}s"" AS src
                WHERE NOT EXISTS (
                    SELECT *
                    FROM ""{1}"".""{0}s"" AS dst 
                    WHERE dst.""{0}_id"" = src.""{0}_id""
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
            var tableName = "NormalizedQueries";

            var sql = @"
                CREATE TEMP TABLE ""{0}"" AS
                SELECT * FROM ""{1}"".""{0}"" WHERE 0=1;
            ";
            sql = string.Format(sql, tableName, ConnectionInfo.SchemaName);

            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;
                cmd.CommandText = sql;
                _ = cmd.ExecuteNonQuery();
            }

            WriteToTable(DataUtils.ToDataTable(values.Where(t => t.Value != null).Select(t => new { sql_hash = t.Value.Hash, normalized_text = t.Value.NormalizedText, example_text = t.Value.ExampleText })), "temp." + tableName);

            sql = @"
                INSERT INTO ""{1}"".""{0}""
                SELECT *
                FROM temp.{0} AS src
                WHERE NOT EXISTS (
                    SELECT *
                    FROM ""{1}"".""{0}"" AS dst 
                    WHERE dst.""sql_hash"" = src.""sql_hash""
                );
            ";
            sql = string.Format(sql, tableName, ConnectionInfo.SchemaName);

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
            var sql_Update = $@"
                UPDATE ""{ConnectionInfo.SchemaName}"".""Intervals""
                SET end_time = ?,
                    duration_minutes = ?
                WHERE interval_id = ?;
            ";
            var sql_Insert = $@"
                INSERT INTO ""{ConnectionInfo.SchemaName}"".""Intervals""
                    (interval_id, end_time, duration_minutes)
                VALUES (?, ?, ?);
            ";

            var interval_id = ComputeIntervalId(intervalTime);

            using (var cmd = _connection.CreateCommand())
            {
                cmd.Transaction = _transaction;

                // UPDATE
                cmd.CommandText = sql_Update;
                cmd.Parameters.Clear();

                _ = cmd.Parameters.Add(new DuckDBParameter { Value = interval_id });
                _ = cmd.Parameters.Add(new DuckDBParameter { Value = intervalTime });
                _ = cmd.Parameters.Add(new DuckDBParameter { Value = Interval });

                var row_count = cmd.ExecuteNonQuery();

                if (row_count == 0)
                {
                    // INSERT
                    cmd.CommandText = sql_Insert;
                    cmd.Parameters.Clear();

                    _ = cmd.Parameters.Add(new DuckDBParameter { Value = interval_id });
                    _ = cmd.Parameters.Add(new DuckDBParameter { Value = intervalTime });
                    _ = cmd.Parameters.Add(new DuckDBParameter { Value = Interval });

                    _ = cmd.ExecuteNonQuery();
                }
            }

            if (!FirstIntervalWritten)
            {
                using (var cmd = _connection.CreateCommand())
                {
                    cmd.Transaction = _transaction;

                    cmd.CommandText = sql_Update;
                    cmd.Parameters.Clear();

                    _ = cmd.Parameters.Add(new DuckDBParameter { Value = interval_id - 1 });
                    _ = cmd.Parameters.Add(new DuckDBParameter { Value = intervalTime.AddSeconds(-1) });
                    _ = cmd.Parameters.Add(new DuckDBParameter { Value = 0 });

                    _ = cmd.ExecuteNonQuery();

                    FirstIntervalWritten = true;
                }
            }

            return interval_id;
        }

        protected override void CreateTargetTables()
        {
            CreateTargetDatabase();

            var sql = File.ReadAllText(WorkloadController.BaseLocation + "\\Consumer\\Analysis\\DuckDBDatabaseSchema.sql");

            sql = sql.Replace("{SchemaName}", ConnectionInfo.SchemaName);

            using (var conn = new DuckDBConnection())
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
                using (var connection = new DuckDBConnection())
                {
                    connection.ConnectionString = ConnectionInfo.ConnectionString();
                    connection.Open();
                    logger.Info($"Database {connection.DataSource} created successfully.");
                }
            }
            catch (DuckDBException e)
            {
                logger.Error("Unable to create the target database for the analysis", e.Message);
            }
            catch (Exception e)
            {
                logger.Error(e.Message);
                throw;
            }
        }

        protected override void PopulateDictionariesFromDatabaseInternal(WorkloadData data)
        {
            using (var conn = new DuckDBConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString();
                conn.Open();

                var sql = $@"SELECT * FROM ""{ConnectionInfo.SchemaName}"".""Applications""";
                AddAllRowsInternal(conn, sql, data.Applications);

                sql = $@"SELECT * FROM ""{ConnectionInfo.SchemaName}"".""Databases""";
                AddAllRowsInternal(conn, sql, data.Databases);

                sql = $@"SELECT * FROM ""{ConnectionInfo.SchemaName}"".""Hosts""";
                AddAllRowsInternal(conn, sql, data.Hosts);

                sql = $@"SELECT * FROM ""{ConnectionInfo.SchemaName}"".""Logins""";
                AddAllRowsInternal(conn, sql, data.Logins);
            }
        }

        protected override void AddAllRowsInternal(DbConnection conn, string sql, Dictionary<string, int> d)
        {
            try
            {
                conn = (DuckDBConnection)conn;
                using (var cmd = conn.CreateCommand()){
                    cmd.CommandText = sql;

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            d[reader.GetString(1)] = reader.GetInt32(0);
                        }
                    }
                }
            }
            catch (DuckDBException e)
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
