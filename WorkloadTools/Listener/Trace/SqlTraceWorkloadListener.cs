using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WorkloadTools.Listener.Trace;
using WorkloadTools.Util;

namespace WorkloadTools.Listener.Trace
{
    public class SqlTraceWorkloadListener : WorkloadListener
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        private static int DEFAULT_TRACE_SIZE_MB = Properties.Settings.Default.SqlTraceWorkloadListener_DEFAULT_TRACE_SIZE_MB;
        private static int DEFAULT_TRACE_ROLLOVER_COUNT = Properties.Settings.Default.SqlTraceWorkloadListener_DEFAULT_TRACE_ROLLOVER_COUNT;
        private static string DEFAULT_LOG_SQL = @"
            DECLARE @defaultLog nvarchar(4000);

            EXEC master.dbo.xp_instance_regread
	            N'HKEY_LOCAL_MACHINE',
	            N'Software\Microsoft\MSSQLServer\MSSQLServer',
	            N'DefaultLog',
	            @defaultLog OUTPUT;

            IF @defaultLog IS NULL
            BEGIN
	            SELECT @defaultLog = REPLACE(physical_name,'mastlog.ldf','') 
	            FROM sys.master_files
	            WHERE name = 'mastlog';
            END

            SELECT @defaultLog AS DefaultLog;
        ";
        private static int DEFAULT_TRACE_INTERVAL_SECONDS = Properties.Settings.Default.SqlTraceWorkloadListener_DEFAULT_TRACE_INTERVAL_SECONDS;
        private static int DEFAULT_TRACE_ROWS_SLEEP_THRESHOLD = Properties.Settings.Default.SqlTraceWorkloadListener_DEFAULT_TRACE_ROWS_SLEEP_THRESHOLD;

        public enum StreamSourceEnum
        {
            StreamFromFile,
            StreamFromTDS
        }

        public enum EventClassEnum : int
        {
            RPC_Completed = 10,
            SQL_BatchCompleted = 12,
            Timeout = 82
        }

        private int traceId = -1;
        private string tracePath;

        // By default, stream from TDS
        public StreamSourceEnum StreamSource { get; set; } = StreamSourceEnum.StreamFromTDS;


        public SqlTraceWorkloadListener()
        {
            Filter = new TraceEventFilter();
            Source = "Listener\\Trace\\sqlworkload.sql";
        }


        public override void Initialize()
        {
            using (SqlConnection conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString;
                conn.Open();

                string traceSql = null;
                try
                {
                    traceSql = File.ReadAllText(Source);

                    // Push Down EventFilters
                    string filters = "";
                    filters += Environment.NewLine + Filter.ApplicationFilter.PushDown();
                    filters += Environment.NewLine + Filter.DatabaseFilter.PushDown();
                    filters += Environment.NewLine + Filter.HostFilter.PushDown();
                    filters += Environment.NewLine + Filter.LoginFilter.PushDown();

                    tracePath = GetSqlDefaultLogPath(conn);
                    traceSql = String.Format(traceSql, DEFAULT_TRACE_SIZE_MB, DEFAULT_TRACE_ROLLOVER_COUNT, Path.Combine(tracePath  ,"sqlworkload"), filters);
                }
                catch (Exception e)
                {
                    throw new ArgumentException("Cannot open the source script to start the sql trace", e);
                }

                int id = GetTraceId(conn, Path.Combine(tracePath, "sqlworkload"));
                if(id > 0)
                {
                    StopTrace(conn, id);
                }

                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = traceSql;
                traceId = (int)cmd.ExecuteScalar();

                // Initialize the source of execution related events
                if(StreamSource == StreamSourceEnum.StreamFromFile)
                    Task.Factory.StartNew(() => ReadEventsFromFile());
                else if (StreamSource == StreamSourceEnum.StreamFromTDS)
                    Task.Factory.StartNew(() => ReadEventsFromTDS());


                // Initialize the source of performance counters events
                Task.Factory.StartNew(() => ReadPerfCountersEvents());

                // Initialize the source of wait stats events
                Task.Factory.StartNew(() => ReadWaitStatsEvents());
            }
        }

        private string GetSqlDefaultLogPath(SqlConnection conn)
        {
            using (SqlCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText = DEFAULT_LOG_SQL;
                return (string)cmd.ExecuteScalar();
            }
        }


        public override WorkloadEvent Read()
        {
            WorkloadEvent result = null;
            while (!Events.TryDequeue(out result))
            {
                if (stopped)
                    return null;

                Thread.Sleep(5);
            }
            return result;
        }


        // Read Workload events directly from the trace files
        // on the server, via local path
        // This method only works when the process is running
        // on the same machine as the SQL Server
        private void ReadEventsFromFile()
        {
            try
            {
                while(!stopped)
                {
                    // get first trace rollover file
                    List<string> files = Directory.GetFiles(tracePath, "sqlworkload*.trc").ToList();
                    files.Sort();
                    string traceFile = files.ElementAt(0);


                    using (TraceFileWrapper reader = new TraceFileWrapper())
                    {
                        reader.InitializeAsReader(traceFile);

                        while (reader.Read() && !stopped)
                        {
                            try
                            {
                                ExecutionWorkloadEvent evt = new ExecutionWorkloadEvent();

                                if (reader.GetValue("EventClass").ToString() == "RPC:Completed")
                                    evt.Type = WorkloadEvent.EventType.RPCCompleted;
                                else if (reader.GetValue("EventClass").ToString() == "SQL:BatchCompleted")
                                    evt.Type = WorkloadEvent.EventType.BatchCompleted;
                                else
                                    evt.Type = WorkloadEvent.EventType.Unknown;
                                evt.ApplicationName = (string)reader.GetValue("ApplicationName");
                                evt.DatabaseName = (string)reader.GetValue("DatabaseName");
                                evt.HostName = (string)reader.GetValue("HostName");
                                evt.LoginName = (string)reader.GetValue("LoginName");
                                evt.SPID = (int?)reader.GetValue("SPID");
                                evt.Text = (string)reader.GetValue("TextData");
                                evt.Reads = (long?)reader.GetValue("Reads");
                                evt.Writes = (long?)reader.GetValue("Writes");
                                evt.CPU = (int?)reader.GetValue("CPU");
                                evt.Duration = (long?)reader.GetValue("Duration");
                                evt.StartTime = DateTime.Now;

                                if (!Filter.Evaluate(evt))
                                    continue;

                                Events.Enqueue(evt);
                            }
                            catch (Exception ex)
                            {
                                logger.Error(ex.Message);

                                if (ex.InnerException != null)
                                    logger.Error(ex.InnerException.Message);
                            }


                        } // while (Read)

                    } // using reader
                    File.Delete(traceFile);
                } // while not stopped
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);

                if (ex.InnerException != null)
                    logger.Error(ex.InnerException.Message);

                Dispose();
            }

        }



        private void ReadEventsFromTDS()
        {
            string sqlReadTrace = @"
                SELECT EventSequence
	                ,Error
	                ,TextData
	                ,BinaryData
	                ,DatabaseID
	                ,HostName
	                ,ApplicationName
	                ,LoginName
	                ,SPID
	                ,Duration
	                ,StartTime
	                ,EndTime
	                ,Reads
	                ,Writes
	                ,CPU
	                ,EventClass
	                ,DatabaseName
                FROM fn_trace_gettable(@path, {0})
            ";

            string sqlPath = @"
                SELECT path
                FROM sys.traces
                WHERE id = @traceId;
            ";

            long lastEvent = -1;
            string lastTraceFile = "";

            try
            {
                while (!stopped)
                {
                    using (SqlConnection conn = new SqlConnection())
                    {
                        conn.ConnectionString = ConnectionInfo.ConnectionString;
                        conn.Open();

                        SqlCommand cmdPath = conn.CreateCommand();
                        cmdPath.CommandText = sqlPath;

                        if(traceId == -1)
                        {
                            traceId = GetTraceId(conn, Path.Combine(tracePath, "sqlworkload"));
                            if (traceId == -1)
                            {
                                throw new InvalidOperationException("The SqlWorkload capture trace is not running.");
                            }
                        }
                        var paramTraceId = cmdPath.Parameters.Add("@traceId", System.Data.SqlDbType.Int);
                        paramTraceId.Value = traceId;



                        string currentTraceFile = null;
                        try
                        {
                            currentTraceFile = (string)cmdPath.ExecuteScalar();
                        }
                        catch(Exception e)
                        {
                            logger.Error(e.StackTrace);
                            throw;
                        }
                        string filesParam = "1";
                        string pathToTraceParam = currentTraceFile;

                        // check if file has changed
                        if (lastTraceFile != currentTraceFile && !String.IsNullOrEmpty(lastTraceFile))
                        {
                            // when the rollover file changes, read from the last read file
                            // up to the end of all rollover files (this is what DEFAULT does)
                            filesParam = "DEFAULT";
                            pathToTraceParam = lastTraceFile;

                            // Check if the previous file still exists
                            using (SqlCommand cmd = conn.CreateCommand())
                            {
                                cmd.CommandText = String.Format(@"
                                    SET NOCOUNT ON;
                                    DECLARE @t TABLE (FileExists bit, FileIsADicrectory bit, ParentDirectoryExists bit);
                                    INSERT @t
                                    EXEC xp_fileexist '{0}';
                                    SELECT FileExists FROM @t;
                                ",lastTraceFile);

                                if (!(bool)cmd.ExecuteScalar())
                                {
                                    pathToTraceParam = Path.Combine(tracePath, "sqlworkload.trc");
                                }
                            }

                        }
                        lastTraceFile = currentTraceFile;

                        String sql = String.Format(sqlReadTrace,filesParam);

                        if(lastEvent > 0)
                        {
                            sql += "WHERE EventSequence > @lastEvent";
                        }

                        using (SqlCommand cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = sql;

                            var paramPath = cmd.Parameters.Add("@path", System.Data.SqlDbType.NVarChar, 255);
                            paramPath.Value = pathToTraceParam;

                            var paramLastEvent = cmd.Parameters.Add("@lastEvent", System.Data.SqlDbType.BigInt);
                            paramLastEvent.Value = lastEvent;

                            int rowsRead = 0;

                            SqlTransformer transformer = new SqlTransformer();

                            using (SqlDataReader reader = cmd.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    if (reader["EventSequence"] != DBNull.Value)
                                        lastEvent = (long)reader["EventSequence"];

                                    ExecutionWorkloadEvent evt = new ExecutionWorkloadEvent();

                                    int eventClass = (int)reader["EventClass"];


                                    if (eventClass == (int)EventClassEnum.RPC_Completed)
                                        evt.Type = WorkloadEvent.EventType.RPCCompleted;
                                    else if (eventClass == (int)EventClassEnum.SQL_BatchCompleted)
                                        evt.Type = WorkloadEvent.EventType.BatchCompleted;
                                    else if (eventClass == (int)EventClassEnum.Timeout)
                                    {
                                        if (reader["TextData"].ToString().StartsWith("WorkloadTools.Timeout["))
                                            evt.Type = WorkloadEvent.EventType.Timeout;
                                    }
                                    else
                                    {
                                        evt.Type = WorkloadEvent.EventType.Unknown;
                                        continue;
                                    }
                                    if (reader["ApplicationName"] != DBNull.Value)
                                        evt.ApplicationName = (string)reader["ApplicationName"];
                                    if (reader["DatabaseName"] != DBNull.Value)
                                        evt.DatabaseName = (string)reader["DatabaseName"];
                                    if (reader["HostName"] != DBNull.Value)
                                        evt.HostName = (string)reader["HostName"];
                                    if (reader["LoginName"] != DBNull.Value)
                                        evt.LoginName = (string)reader["LoginName"];
                                    evt.SPID = (int?)reader["SPID"];
                                    if (reader["TextData"] != DBNull.Value)
                                        evt.Text = (string)reader["TextData"];
                                   
                                    evt.StartTime = (DateTime)reader["StartTime"];

                                    if(evt.Type == WorkloadEvent.EventType.Timeout)
                                    {
                                        if (reader["BinaryData"] != DBNull.Value)
                                        {
                                            byte[] bytes = (byte[])reader["BinaryData"];
                                            evt.Text = Encoding.Unicode.GetString(bytes);
                                        }
                                        evt.Duration = ExtractTimeoutDuration(reader["TextData"]);
                                        evt.CPU = Convert.ToInt32(evt.Duration / 1000);
                                    }
                                    else
                                    {
                                        evt.Reads = (long?)reader["Reads"];
                                        evt.Writes = (long?)reader["Writes"];
                                        evt.CPU = (int?)reader["CPU"];
                                        evt.Duration = (long?)reader["Duration"];
                                    }

                                    if (transformer.Skip(evt.Text))
                                        continue;

                                    if (!Filter.Evaluate(evt))
                                        continue;

                                    evt.Text = transformer.Transform(evt.Text);

                                    Events.Enqueue(evt);

                                    rowsRead++;
                                }
                            }

                            // Wait before querying the trace file again
                            if (rowsRead < DEFAULT_TRACE_ROWS_SLEEP_THRESHOLD)
                                Thread.Sleep(DEFAULT_TRACE_INTERVAL_SECONDS * 1000);

                        }

                    }
                }
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);
                logger.Error(ex.StackTrace);

                if (ex.InnerException != null)
                    logger.Error(ex.InnerException.Message);

                Dispose();
            }

        }

        private long? ExtractTimeoutDuration(object textData)
        {
            long result = 30;
            if(textData != DBNull.Value)
            {
                string description = (string)textData;
                string durationAsString = new String(description.Where(Char.IsDigit).ToArray());
                result = Convert.ToInt64(durationAsString);
            }
            return result * 1000 * 1000;
        }

        protected override void Dispose(bool disposing)
        {
            stopped = true;
            using (SqlConnection conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString;
                conn.Open();
                StopTrace(conn, traceId);
            }
            logger.Info("Trace with id={0} stopped successfully.", traceId);
        }


        private void StopTrace(SqlConnection conn, int id)
        {
                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = String.Format(@"
                    IF EXISTS (
                        SELECT *
                        FROM sys.traces
                        WHERE id = {0}
                    )
                    BEGIN
                        EXEC sp_trace_setstatus {0}, 0;
                        EXEC sp_trace_setstatus {0}, 2;
                    END
                ", id);
                cmd.ExecuteNonQuery();
        }


        private int GetTraceId(SqlConnection conn, string path)
        {
            string sql = @"
                SELECT TOP(1) id
                FROM (
	                SELECT id FROM sys.traces WHERE path LIKE '{0}%'
	                UNION ALL
	                SELECT -1
                ) AS i
                ORDER BY id DESC
            ";

            SqlCommand cmd = conn.CreateCommand();
            cmd.CommandText = String.Format(sql, path);
            return (int)cmd.ExecuteScalar();
        }




    }
}
