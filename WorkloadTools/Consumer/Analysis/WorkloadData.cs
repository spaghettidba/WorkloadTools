using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using static WorkloadTools.Consumer.Analysis.WorkloadAnalyzer;

namespace WorkloadTools.Consumer.Analysis
{
    public class WorkloadData : IDisposable
    {
        public DataTable ErrorData { get; set; }
        
        
        public DataTable CounterData { get; set; }
        public DataTable WaitsData { get; set; }
        public DataTable DiskPerfData { get; set; }
        public SqlTextNormalizer Normalizer { get; set; }

        public Dictionary<long, NormalizedQuery> NormalizedQueries { get; } = new Dictionary<long, NormalizedQuery>();
        public Dictionary<string, int> Applications { get; } = new Dictionary<string, int>();
        public Dictionary<string, int> Databases { get; } = new Dictionary<string, int>();
        public Dictionary<string, int> Logins { get; } = new Dictionary<string, int>();
        public Dictionary<string, int> Hosts { get; } = new Dictionary<string, int>();

        public ConcurrentDictionary<ExecutionDetailKey, List<ExecutionDetailValue>> RawData { get; set; }

        private readonly AnalysisDatabaseWriter databaseWriter;
        private readonly SqlConnectionInfo ConnectionInfo;

        public WorkloadData() {
            if (RawData == null)
            {
                PrepareDataTables();
            }
        }

        public void InternalAdd(WorkloadEvent evt)
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
            var row = ErrorData.NewRow();
            row.SetField("message", evt.Text);
            row.SetField("type", evt.Type);
            ErrorData.Rows.Add(row);
        }

        private void InternalAdd(WaitStatsWorkloadEvent evt)
        {
            if (WaitsData == null)
            {
                WaitsData = evt.Waits;
            }
            else
            {
                WaitsData.Merge(evt.Waits);
            }
        }

        private void InternalAdd(DiskPerfWorkloadEvent evt)
        {
            if (DiskPerfData == null)
            {
                DiskPerfData = evt.DiskPerf;
            }
            else
            {
                DiskPerfData.Merge(evt.DiskPerf);
            }
        }

        private void InternalAdd(CounterWorkloadEvent evt)
        {
            if (CounterData == null)
            {
                CounterData = new DataTable();

                _ = CounterData.Columns.Add("event_time", typeof(DateTime));
                _ = CounterData.Columns.Add("counter_name", typeof(string));
                _ = CounterData.Columns.Add("counter_value", typeof(float));
            }

            foreach (var cntr in evt.Counters.Keys)
            {
                var row = CounterData.NewRow();

                row.SetField("event_time", evt.StartTime);
                row.SetField("counter_name", cntr.ToString());
                row.SetField("counter_value", evt.Counters[cntr]);

                CounterData.Rows.Add(row);
            }

        }

        private void InternalAdd(ExecutionWorkloadEvent evt)
        {
            if (RawData == null)
            {
                PrepareDataTables();
                PrepareDictionaries();
            }

            var norm = Normalizer.NormalizeSqlText(evt.Text, (int)evt.SPID);

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

            var hash = Normalizer.GetHashCode(normSql);

            if (!NormalizedQueries.ContainsKey(hash))
            {
                NormalizedQueries.Add(hash, new NormalizedQuery { Hash = hash, NormalizedText = normSql, ExampleText = evt.Text });
            }

            var appId = -1;
            if (evt.ApplicationName != null && !Applications.TryGetValue(evt.ApplicationName, out appId))
            {
                Applications.Add(evt.ApplicationName, appId = Applications.Count);
            }

            var dbId = -1;
            if (evt.DatabaseName != null && !Databases.TryGetValue(evt.DatabaseName, out dbId))
            {
                Databases.Add(evt.DatabaseName, dbId = Databases.Count);
            }

            var hostId = -1;
            if (evt.HostName != null && !Hosts.TryGetValue(evt.HostName, out hostId))
            {
                Hosts.Add(evt.HostName, hostId = Hosts.Count);
            }

            var loginId = -1;
            if (evt.LoginName != null && !Logins.TryGetValue(evt.LoginName, out loginId))
            {
                Logins.Add(evt.LoginName, loginId = Logins.Count);
            }

            var theKey = new ExecutionDetailKey()
            {
                Sql_hash = hash,
                Application_id = appId,
                Database_id = dbId,
                Host_id = hostId,
                Login_id = loginId
            };
            var theValue = new ExecutionDetailValue()
            {
                Event_time = evt.StartTime,
                Cpu_us = evt.CPU,
                Reads = evt.Reads,
                Writes = evt.Writes,
                Duration_us = evt.Duration
            };

            // Look up execution detail 
            if (RawData.TryGetValue(theKey, out var theList))
            {
                if (theList == null)
                {
                    theList = new List<ExecutionDetailValue>();
                }
                theList.Add(theValue);
            }
            else
            {
                theList = new List<ExecutionDetailValue>
                {
                    theValue
                };

                if (!RawData.TryAdd(theKey, theList))
                {
                    throw new InvalidOperationException("Unable to add an executionEvent to the queue");
                }
            }
        }


        public void PrepareDataTables()
        {
            RawData = new ConcurrentDictionary<ExecutionDetailKey, List<ExecutionDetailValue>>();
            ErrorData = new DataTable();
            _ = ErrorData.Columns.Add("type", typeof(int));
            _ = ErrorData.Columns.Add("message", typeof(string));
        }

        public void PrepareDictionaries()
        {
            databaseWriter.CreateTargetDatabase();

            using (var conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString();
                conn.Open();

                var sql = string.Format(@"SELECT * FROM [{0}].[Applications]", ConnectionInfo.SchemaName);
                databaseWriter.AddAllRows(conn, sql, Applications);

                sql = string.Format(@"SELECT * FROM [{0}].[Databases]", ConnectionInfo.SchemaName);
                databaseWriter.AddAllRows(conn, sql, Databases);

                sql = string.Format(@"SELECT * FROM [{0}].[Hosts]", ConnectionInfo.SchemaName);
                databaseWriter.AddAllRows(conn, sql, Hosts);

                sql = string.Format(@"SELECT * FROM [{0}].[Logins]", ConnectionInfo.SchemaName);
                databaseWriter.AddAllRows(conn, sql, Logins);
            }
        }

        public void Dispose()
        {
            RawData?.Clear();
            ErrorData?.Dispose();
            CounterData?.Dispose();
            WaitsData?.Dispose();
        }

        internal void ClearRawData()
        {
            RawData?.Clear();
        }
    }
}
