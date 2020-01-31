using NLog;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WorkloadTools.Listener.Trace
{
    public class FileTraceEventDataReader : TraceEventDataReader
    {

        public enum EventClassEnum : int
        {
            RPC_Completed = 10,
            SQL_BatchCompleted = 12,
            Timeout = 82
        }

        private static Logger logger = LogManager.GetCurrentClassLogger();

        private int TraceRowsSleepThreshold { get; set; } = 5000;
        private int TraceIntervalSeconds { get; set; } = 10;

        private bool stopped = false;
        private int traceId = -1;
        private string tracePath;

        private TraceUtils utils;

        public SqlConnectionInfo ConnectionInfo { get; set; }

        public FileTraceEventDataReader(string connectionString, WorkloadEventFilter filter, IEventQueue events) : base(connectionString, filter, events)
        {
            utils = new TraceUtils();
        }


        public override void ReadEvents()
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

                        if (traceId == -1)
                        {
                            traceId = utils.GetTraceId(conn, Path.Combine(tracePath, "sqlworkload"));
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
                        catch (Exception e)
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
                                ", lastTraceFile);

                                if (!(bool)cmd.ExecuteScalar())
                                {
                                    pathToTraceParam = Path.Combine(tracePath, "sqlworkload.trc");
                                }
                            }

                        }
                        lastTraceFile = currentTraceFile;

                        String sql = String.Format(sqlReadTrace, filesParam);

                        if (lastEvent > 0)
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

                                    if (evt.Type == WorkloadEvent.EventType.Timeout)
                                    {
                                        if (reader["BinaryData"] != DBNull.Value)
                                        {
                                            byte[] bytes = (byte[])reader["BinaryData"];
                                            evt.Text = Encoding.Unicode.GetString(bytes);
                                        }
                                        evt.Duration = ExtractTimeoutDuration(reader["TextData"]);
                                        evt.CPU = Convert.ToInt64(evt.Duration);
                                    }
                                    else
                                    {
                                        evt.Reads = (long?)reader["Reads"];
                                        evt.Writes = (long?)reader["Writes"];
                                        evt.CPU = (long?)Convert.ToInt64(reader["CPU"]) * 1000; // SqlTrace captures CPU as milliseconds => convert to microseconds
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
                            if (rowsRead < TraceRowsSleepThreshold)
                                Thread.Sleep(TraceIntervalSeconds * 1000);

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

        public override void Stop()
        {
            stopped = true;
        }

        private long? ExtractTimeoutDuration(object textData)
        {
            long result = 30;
            if (textData != DBNull.Value)
            {
                string description = (string)textData;
                string durationAsString = new String(description.Where(Char.IsDigit).ToArray());
                result = Convert.ToInt64(durationAsString);
            }
            return result * 1000 * 1000;
        }
    }
}
