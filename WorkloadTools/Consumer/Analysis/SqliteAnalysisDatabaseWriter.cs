using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Data.Sqlite;

using NLog;

using WorkloadTools.Util;

namespace WorkloadTools.Consumer.Analysis
{
    internal class SqliteAnalysisDatabaseWriter : AnalysisDatabaseWriter
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly WorkloadSummaryAggregator _aggregator = new WorkloadSummaryAggregator();

        private Microsoft.Data.Sqlite.SqliteConnection _connection;

        private void WriteToDatabase(DataTable dt, string tableName)
        {
            using (var transaction = _connection.BeginTransaction())
            {
                try
                {
                    using (var command = new Microsoft.Data.Sqlite.SqliteCommand())
                    {
                        command.Connection = _connection;
                        command.Transaction = transaction;

                        var columns = new System.Collections.Generic.List<string>();
                        var schemaCommand = _connection.CreateCommand();
                        schemaCommand.Transaction = transaction;
                        schemaCommand.CommandText = $"PRAGMA table_info([{tableName}]);";

                        using (var reader = schemaCommand.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                columns.Add(reader.GetString(1));
                            }
                        }

                        var columnNames = string.Join(", ", columns.ConvertAll(c => $"[{c}]"));
                        var paramNames = string.Join(", ", columns.ConvertAll(c => $"${c}"));
                        command.CommandText = $"INSERT INTO [{tableName}] ({columnNames}) VALUES ({paramNames});";

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

                    transaction.Commit();
                    logger.Info($"{tableName} written to SQLite");
                }
                catch (Exception)
                {
                    transaction.Rollback();
                    throw;
                }
            }
        }

        public override void WriteToServer(DateTime intervalTime)
        {
            logger.Warn("SQLite database writing is not yet implemented");
            throw new NotImplementedException("SQLite support for analysis database writing is not yet implemented");
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
            //var summaryRecords = _aggregator.AggregateExecutionSummary(Data)

        }

        protected override void WriteExecutionDetails(int current_interval_id)
        {
            throw new NotImplementedException("SQLite support for analysis database writing is not yet implemented");
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
            throw new NotImplementedException("SQLite support for analysis database writing is not yet implemented");
        }

        protected override void WriteNormalizedQueries(Dictionary<long, NormalizedQuery> values)
        {
            throw new NotImplementedException("SQLite support for analysis database writing is not yet implemented");
        }

        protected override int CreateInterval(DateTime intervalTime)
        {
            throw new NotImplementedException("SQLite support for analysis database writing is not yet implemented");
        }

        protected override void CreateTargetTables()
        {
            throw new NotImplementedException("SQLite support for analysis database writing is not yet implemented");
        }

        protected override void CreateTargetDatabase()
        {
            throw new NotImplementedException("SQLite support for analysis database writing is not yet implemented");
        }

        protected override void PopulateDictionariesFromDatabaseInternal(WorkloadData data)
        {
            throw new NotImplementedException("SQLite support for analysis database writing is not yet implemented");
        }

        protected override void AddAllRowsInternal(SqlConnection conn, string sql, Dictionary<string, int> d)
        {
            throw new NotImplementedException("SQLite support for analysis database writing is not yet implemented");
        }
    }
}
