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

namespace WorkloadTools.Consumer.Analysis
{
    public class WorkloadAnalyzer
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        public SqlConnectionInfo ConnectionInfo { get; set; }
        public int Interval { get; set; }

        private Dictionary<long, NormalizedQuery> normalizedQueries = new Dictionary<long, NormalizedQuery>();
        private Dictionary<string, int> applications = new Dictionary<string, int>();
        private Dictionary<string, int> databases = new Dictionary<string, int>();
        private Dictionary<string, int> logins = new Dictionary<string, int>();
        private Dictionary<string, int> hosts = new Dictionary<string, int>();

        private Queue<WorkloadEvent> _internalQueue = new Queue<WorkloadEvent>();
        private Thread Worker;
        private bool stopped = false;

        private DataTable rawData;
		private DataTable errorData;
        private SqlTextNormalizer normalizer;
        private bool TargetTableCreated = false;
        private bool FirstIntervalWritten = false;

        private DataTable counterData;
        private DataTable waitsData;

        public int MaximumWriteRetries { get; set; }
		public bool TruncateTo4000 { get; set; }
		public bool TruncateTo1024 { get; set; }

		private class NormalizedQuery
        {
            public long Hash { get; set; }
            public string NormalizedText { get; set; }
            public string ExampleText { get; set; }
        }


		public WorkloadAnalyzer()
		{
			normalizer = new SqlTextNormalizer()
			{
				TruncateTo1024 = this.TruncateTo1024,
				TruncateTo4000 = this.TruncateTo4000
			};
		}


        private void ProcessQueue()
        {
            DateTime lastDump = DateTime.Now;
            while (!stopped)
            {
                // Write collected data to the destination database
                TimeSpan duration = DateTime.Now - lastDump;
                if (duration.TotalMinutes >= Interval)
                {
                    try
                    {
                        int numRetries = 0;
                        while (numRetries <= MaximumWriteRetries)
                        {
                            try
                            {
                                WriteToServer();
                                numRetries = MaximumWriteRetries + 1;
                            }
                            catch (Exception ex)
                            {
                                logger.Warn("Unable to write workload analysis.");
                                logger.Warn(ex.Message);

                                if (numRetries == MaximumWriteRetries)
                                    throw;
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
                            Console.WriteLine(String.Format("Unable to write to the database: {0}.", e.Message));
                        }
                    }
                    finally
                    {
                        lastDump = DateTime.Now;
                    }
                }

                if (_internalQueue.Count == 0)
                {
                    Thread.Sleep(10);
                    continue;
                }
                lock (_internalQueue)
                {
                    WorkloadEvent data = _internalQueue.Dequeue();
                    _internalAdd(data);
                }
            }
        }


        public void Add(WorkloadEvent evt)
        {
            if (evt is ExecutionWorkloadEvent && String.IsNullOrEmpty(((ExecutionWorkloadEvent)evt).Text))
                return;

            lock (_internalQueue)
            {
                _internalQueue.Enqueue(evt);
            }

            bool startNewWorker = false;
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
                });
                Worker.IsBackground = true;
                Worker.Name = "RealtimeWorkloadAnalyzer.Worker";
                Worker.Start();

                Thread.Sleep(100);
            }
        }



        private void _internalAdd(WorkloadEvent evt)
        {
            if (evt is ExecutionWorkloadEvent)
                _internalAdd((ExecutionWorkloadEvent)evt);
			if (evt is ErrorWorkloadEvent)
				_internalAdd((ErrorWorkloadEvent)evt);
			if (evt is CounterWorkloadEvent)
                _internalAdd((CounterWorkloadEvent)evt);
            if (evt is WaitStatsWorkloadEvent)
                _internalAdd((WaitStatsWorkloadEvent)evt);
        }

		private void _internalAdd(ErrorWorkloadEvent evt)
		{
			DataRow row = errorData.NewRow();
			row.SetField("message", evt.Text);
			errorData.Rows.Add(row);
		}

		private void _internalAdd(WaitStatsWorkloadEvent evt)
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


        private void _internalAdd(CounterWorkloadEvent evt)
        {
            if (counterData == null)
            {
                counterData = new DataTable();

                counterData.Columns.Add("event_time", typeof(DateTime));
                counterData.Columns.Add("counter_name", typeof(string));
                counterData.Columns.Add("counter_value", typeof(float));
            }

            foreach(var cntr in evt.Counters.Keys)
            {
                DataRow row = counterData.NewRow();

                row.SetField("event_time", evt.StartTime);
                row.SetField("counter_name", cntr.ToString());
                row.SetField("counter_value", evt.Counters[cntr]);

                counterData.Rows.Add(row);
            }
            
        }


        private void _internalAdd(ExecutionWorkloadEvent evt)
        {
            if (rawData == null)
            {
                PrepareDataTables();
                PrepareDictionaries();
            }

            DataRow row = rawData.NewRow();

            string normSql = null;
            var norm = normalizer.NormalizeSqlText(evt.Text, (int)evt.SPID);

            if (norm != null)
                normSql = norm.NormalizedText;
            else
                return;

            if (normSql == null)
                return;

            long hash = normalizer.GetHashCode(normSql);

            if (!normalizedQueries.ContainsKey(hash))
            {
                normalizedQueries.Add(hash, new NormalizedQuery { Hash = hash, NormalizedText = normSql, ExampleText = evt.Text });
            }

            int appId = -1;
            if (evt.ApplicationName != null && !applications.TryGetValue(evt.ApplicationName, out appId))
            {
                applications.Add(evt.ApplicationName, appId = applications.Count);
            }

            int dbId = -1;
            if (evt.DatabaseName != null && !databases.TryGetValue(evt.DatabaseName, out dbId))
            {
                databases.Add(evt.DatabaseName, dbId = databases.Count);
            }

            int hostId = -1;
            if (evt.HostName != null && !hosts.TryGetValue(evt.HostName, out hostId))
            {
                hosts.Add(evt.HostName, hostId = hosts.Count);
            }

            int loginId = -1;
            if (evt.LoginName != null && !logins.TryGetValue(evt.LoginName, out loginId))
            {
                logins.Add(evt.LoginName, loginId = logins.Count);
            }

            row.SetField("sql_hash", hash);
            row.SetField("application_id", appId);
            row.SetField("database_id", dbId);
            row.SetField("host_id", hostId);
            row.SetField("login_id", loginId);
            row.SetField("event_time", evt.StartTime);
            row.SetField("cpu_ms", evt.CPU / 1000); // microseconds to milliseconds
            row.SetField("reads", evt.Reads);
            row.SetField("writes", evt.Writes);
            row.SetField("duration_ms", evt.Duration / 1000); // microseconds to milliseconds

            rawData.Rows.Add(row);
        }

        public void Stop()
        {
            WriteToServer();
            stopped = true;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private void WriteToServer()
        {
            logger.Trace("Writing Workload Analysis data");


            using (SqlConnection conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString;
                conn.Open();

                if (!TargetTableCreated)
                {
                    CreateTargetTables();
                    TargetTableCreated = true;
                }

                var tran = conn.BeginTransaction();

                try
                {
                    int current_interval_id = CreateInterval(conn, tran);

                    WriteDictionary(applications, conn, tran, "applications");
                    WriteDictionary(databases, conn, tran, "databases");
                    WriteDictionary(hosts, conn, tran, "hosts");
                    WriteDictionary(logins, conn, tran, "logins");
                    WriteNormalizedQueries(normalizedQueries, conn, tran);

                    WriteExecutionDetails(conn, tran, current_interval_id);
					WriteExecutionErrors(conn, tran, current_interval_id);
					WritePerformanceCounters(conn, tran, current_interval_id);
                    WriteWaitsData(conn, tran, current_interval_id);

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
                return;

            lock (waitsData)
            {
                using (SqlBulkCopy bulkCopy = new System.Data.SqlClient.SqlBulkCopy(conn,
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

        private void WritePerformanceCounters(SqlConnection conn, SqlTransaction tran, int current_interval_id)
        {
            if (counterData == null)
                return;

            lock (counterData)
            {
                using (SqlBulkCopy bulkCopy = new System.Data.SqlClient.SqlBulkCopy(conn,
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

        private void WriteExecutionDetails(SqlConnection conn, SqlTransaction tran, int current_interval_id)
        {
            int numRows;

            if(rawData == null)
                PrepareDataTables();

            lock (rawData)
            {
                using (SqlBulkCopy bulkCopy = new System.Data.SqlClient.SqlBulkCopy(conn,
                                                SqlBulkCopyOptions.KeepIdentity |
                                                SqlBulkCopyOptions.FireTriggers |
                                                SqlBulkCopyOptions.CheckConstraints |
                                                SqlBulkCopyOptions.TableLock,
                                                tran))
                {

                    bulkCopy.DestinationTableName = "[" + ConnectionInfo.SchemaName + "].[WorkloadDetails]";
                    bulkCopy.BatchSize = 1000;
                    bulkCopy.BulkCopyTimeout = 300;


                    var Table = from t in rawData.AsEnumerable()
                                group t by new
                                {
                                    sql_hash = t.Field<long>("sql_hash"),
                                    application_id = t.Field<int>("application_id"),
                                    database_id = t.Field<int>("database_id"),
                                    host_id = t.Field<int>("host_id"),
                                    login_id = t.Field<int>("login_id")
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

                                    avg_cpu_ms = grp.Average(t => t.Field<long?>("cpu_ms")),
                                    min_cpu_ms = grp.Min(t => t.Field<long?>("cpu_ms")),
                                    max_cpu_ms = grp.Max(t => t.Field<long?>("cpu_ms")),
                                    sum_cpu_ms = grp.Sum(t => t.Field<long?>("cpu_ms")),

                                    avg_reads = grp.Average(t => t.Field<long?>("reads")),
                                    min_reads = grp.Min(t => t.Field<long?>("reads")),
                                    max_reads = grp.Max(t => t.Field<long?>("reads")),
                                    sum_reads = grp.Sum(t => t.Field<long?>("reads")),

                                    avg_writes = grp.Average(t => t.Field<long?>("writes")),
                                    min_writes = grp.Min(t => t.Field<long?>("writes")),
                                    max_writes = grp.Max(t => t.Field<long?>("writes")),
                                    sum_writes = grp.Sum(t => t.Field<long?>("writes")),

                                    avg_duration_ms = grp.Average(t => t.Field<long?>("duration_ms")),
                                    min_duration_ms = grp.Min(t => t.Field<long?>("duration_ms")),
                                    max_duration_ms = grp.Max(t => t.Field<long?>("duration_ms")),
                                    sum_duration_ms = grp.Sum(t => t.Field<long?>("duration_ms")),

                                    execution_count = grp.Count()
                                };

                    bulkCopy.WriteToServer(DataUtils.ToDataTable(Table));
                    numRows = rawData.Rows.Count;
                    logger.Info(String.Format("{0} rows aggregated", numRows));
                    numRows = Table.Count();
                    logger.Info(String.Format("{0} rows written", numRows));
                }
                rawData.Rows.Clear();
            }
        }

		private void WriteExecutionErrors(SqlConnection conn, SqlTransaction tran, int current_interval_id)
		{

			if (errorData == null)
				PrepareDataTables();

			lock (errorData)
			{
				using (SqlBulkCopy bulkCopy = new System.Data.SqlClient.SqlBulkCopy(conn,
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
									message = t.Field<long>("message")
								}
								into grp
								select new
								{
									interval_id = current_interval_id,
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

            string sql = @"
                SELECT TOP(0) *
                INTO #{0}
                FROM [{1}].[{0}];
            ";
            sql = String.Format(sql, name, ConnectionInfo.SchemaName);

            using (SqlCommand cmd = conn.CreateCommand())
            {
                cmd.Transaction = tran;
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }

            // bulk insert into temporary
            using (SqlBulkCopy bulkCopy = new System.Data.SqlClient.SqlBulkCopy(conn,
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
            sql = String.Format(sql, name.Substring(0, name.Length - 1), ConnectionInfo.SchemaName);

            using (SqlCommand cmd = conn.CreateCommand())
            {
                cmd.Transaction = tran;
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }
        }


        private void WriteNormalizedQueries(Dictionary<long, NormalizedQuery> values, SqlConnection conn, SqlTransaction tran)
        {
            // create a temporary table

            string sql = @"
                SELECT TOP(0) *
                INTO #NormalizedQueries
                FROM [{0}].[NormalizedQueries];
            ";
            sql = String.Format(sql, ConnectionInfo.SchemaName);

            using (SqlCommand cmd = conn.CreateCommand())
            {
                cmd.Transaction = tran;
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }

            // bulk insert into temporary
            using (SqlBulkCopy bulkCopy = new System.Data.SqlClient.SqlBulkCopy(conn,
                                                                SqlBulkCopyOptions.KeepIdentity |
                                                                SqlBulkCopyOptions.FireTriggers |
                                                                SqlBulkCopyOptions.CheckConstraints |
                                                                SqlBulkCopyOptions.TableLock,
                                                                tran))
            {

                bulkCopy.DestinationTableName = "#NormalizedQueries";
                bulkCopy.BatchSize = 1000;
                bulkCopy.BulkCopyTimeout = 300;
                bulkCopy.WriteToServer(DataUtils.ToDataTable(from t in values where (t.Value != null) select new { t.Value.Hash, t.Value.NormalizedText, t.Value.ExampleText }));

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
            sql = String.Format(sql, ConnectionInfo.SchemaName);

            using (SqlCommand cmd = conn.CreateCommand())
            {
                cmd.Transaction = tran;
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }


            // Erase from memory all the normalized queries 
            // already written to the database. This should reduce
            // the memory footprint quite a lot
            foreach(var hash in values.Keys.ToList())
            {
                values[hash] = null;
            }
            // Run the Garbage Collector in a separate task
            Task.Factory.StartNew(() => InvokeGC());
        }


        private void InvokeGC()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }


        private int CreateInterval(SqlConnection conn, SqlTransaction tran)
        {
            string sql = @"INSERT INTO [{0}].[Intervals] (interval_id, end_time, duration_minutes) VALUES (@interval_id, @end_time, @duration_minutes); ";
            sql = String.Format(sql, ConnectionInfo.SchemaName);

            // interval id is the number of seconds since 01/01/2000
            int interval_id = (int)DateTime.Now.Subtract(DateTime.MinValue.AddYears(1999)).TotalSeconds;

            using (SqlCommand cmd = conn.CreateCommand())
            {
                cmd.Transaction = tran;
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("@interval_id", interval_id);
                cmd.Parameters.AddWithValue("@end_time", DateTime.Now);
                cmd.Parameters.AddWithValue("@duration_minutes", Interval);
                cmd.ExecuteNonQuery();
            }

            // If this the first interval of the analysis, write
            // a marker interval with duration = 0 
            if (!FirstIntervalWritten)
            {
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.Transaction = tran;
                    cmd.CommandText = sql;
                    cmd.Parameters.AddWithValue("@interval_id", interval_id - 1);
                    cmd.Parameters.AddWithValue("@end_time", DateTime.Now.AddSeconds(-1));
                    cmd.Parameters.AddWithValue("@duration_minutes", 0);
                    cmd.ExecuteNonQuery();
                    FirstIntervalWritten = true;
                }
            }

            return interval_id;
        }

        private void PrepareDataTables()
        {
            rawData = new DataTable();

            rawData.Columns.Add("sql_hash", typeof(long));
            rawData.Columns.Add("application_id", typeof(int));
            rawData.Columns.Add("database_id", typeof(int));
            rawData.Columns.Add("host_id", typeof(int));
            rawData.Columns.Add("login_id", typeof(int));
            rawData.Columns.Add("event_time", typeof(DateTime));
            rawData.Columns.Add("cpu_ms", typeof(long));
            rawData.Columns.Add("reads", typeof(long));
            rawData.Columns.Add("writes", typeof(long));
            rawData.Columns.Add("duration_ms", typeof(long));

			errorData = new DataTable();
			errorData.Columns.Add("message", typeof(string));
		}


        private void PrepareDictionaries()
        {
			CreateTargetDatabase();

			using (SqlConnection conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString;
                conn.Open();

                string sql = String.Format(@"SELECT * FROM [{0}].[Applications]",ConnectionInfo.SchemaName);
                AddAllRows(conn, sql, applications);

                sql = String.Format(@"SELECT * FROM [{0}].[Databases]", ConnectionInfo.SchemaName);
                AddAllRows(conn, sql, databases);

                sql = String.Format(@"SELECT * FROM [{0}].[Hosts]", ConnectionInfo.SchemaName);
                AddAllRows(conn, sql, hosts);

                sql = String.Format(@"SELECT * FROM [{0}].[Logins]", ConnectionInfo.SchemaName);
                AddAllRows(conn, sql, logins);
            }
        }

        private void AddAllRows(SqlConnection conn, string sql,  Dictionary<string, int> d)
        {
            try
            {

                using (SqlDataAdapter adapter = new SqlDataAdapter(sql, conn))
                {
                    using (DataSet ds = new DataSet())
                    {
                        adapter.Fill(ds);
                        DataTable dt = ds.Tables[0];
                        foreach (DataRow dr in dt.Rows)
                        {
                            d.Add((string)dr[1], (int)dr[0]);
                        }
                    }
                }
            }
            catch(SqlException e)
            {
                logger.Trace("Unable to read saved classifiers from the analyssi database: {0}", e.Message);
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

			string sql = File.ReadAllText(WorkloadController.BaseLocation + "\\Consumer\\Analysis\\DatabaseSchema.sql");

            sql = sql.Replace("{DatabaseName}", ConnectionInfo.DatabaseName);
            sql = sql.Replace("{SchemaName}", ConnectionInfo.SchemaName);

            using (SqlConnection conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString;
                conn.Open();
                conn.ChangeDatabase(ConnectionInfo.DatabaseName);

                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                }

                sql = "IF OBJECT_ID('dbo.createAnalysisView') IS NOT NULL EXEC('DROP PROCEDURE dbo.createAnalysisView')";
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                }

                sql = File.ReadAllText(WorkloadController.BaseLocation + "\\Consumer\\Analysis\\createAnalysisView.sql");
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                }

                // Invoke the stored procedure to create the workload comparison view
                sql = @"
                    DECLARE @name1 sysname, @name2 sysname;

                    SELECT @name1 = ISNULL([1],'dbo'), @name2 = ISNULL([2],'dbo')
                    FROM (
                        SELECT TOP(2) OBJECT_SCHEMA_NAME(object_id) AS schema_name, ROW_NUMBER() OVER (ORDER BY create_date DESC) AS RN
                        FROM sys.tables
                        WHERE name = 'WorkloadDetails'
                        ORDER BY create_date DESC
                    ) AS src
                    PIVOT( MIN(schema_name) FOR RN IN ([1], [2])) AS p;

                    IF OBJECT_ID(@name1 + '.WorkloadDetails') IS NOT NULL
                        AND OBJECT_ID(@name2 + '.WorkloadDetails') IS NOT NULL
                    BEGIN
                        EXEC createAnalysisView @name1, @name2;
                    END
                ";
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                }
            }
        }


		protected void CreateTargetDatabase()
		{
			string databaseName = ConnectionInfo.DatabaseName;
			ConnectionInfo.DatabaseName = "master";

			try
			{
				using (SqlConnection conn = new SqlConnection())
				{
					conn.ConnectionString = ConnectionInfo.ConnectionString;
					conn.Open();
					conn.ChangeDatabase(ConnectionInfo.DatabaseName);
					using (SqlCommand cmd = conn.CreateCommand())
					{
						string createDb = @"
						IF DB_ID(@name) IS NULL 
						BEGIN
						    DECLARE @sql nvarchar(max); 
							SET @sql = N'CREATE DATABASE ' + QUOTENAME(@name);
							EXEC sp_executesql @sql;
						END
					";
						cmd.CommandText = createDb;
						cmd.Parameters.AddWithValue("@name", databaseName);
						cmd.ExecuteNonQuery();
					}
				}
			}
			finally
			{
				// restore original database name
				ConnectionInfo.DatabaseName = databaseName;
			}

		}


    }
}


