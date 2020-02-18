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


        public enum StreamSourceEnum
        {
            StreamFromFile,
            StreamFromTDS
        }

        

        private int traceId = -1;
        private string tracePath;

        // By default, stream from TDS
        public StreamSourceEnum StreamSource { get; set; } = StreamSourceEnum.StreamFromTDS;

		public int TraceSizeMB { get; set; } = 10;
		public int TraceRolloverCount { get; set; } = 30;
		
		

        private TraceUtils utils;

		public SqlTraceWorkloadListener() : base()
        {
            Filter = new TraceEventFilter();
            Source = WorkloadController.BaseLocation + "\\Listener\\Trace\\sqlworkload.sql";
            utils = new TraceUtils();
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
                    traceSql = System.IO.File.ReadAllText(Source);

                    // Push Down EventFilters
                    string filters = "";
                    filters += Environment.NewLine + Filter.ApplicationFilter.PushDown();
                    filters += Environment.NewLine + Filter.DatabaseFilter.PushDown();
                    filters += Environment.NewLine + Filter.HostFilter.PushDown();
                    filters += Environment.NewLine + Filter.LoginFilter.PushDown();

                    tracePath = utils.GetSqlDefaultLogPath(conn);
                    traceSql = String.Format(traceSql, TraceSizeMB, TraceRolloverCount, Path.Combine(tracePath  ,"sqlworkload"), filters);
                }
                catch (Exception e)
                {
                    throw new ArgumentException("Cannot open the source script to start the sql trace", e);
                }

                int id = utils.GetTraceId(conn, Path.Combine(tracePath, "sqlworkload"));
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


        public override WorkloadEvent Read()
        {
            try
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
            catch (Exception)
            {
                if (stopped) return null;
                else throw;
            }
        }


        private void ReadEventsFromTDS()
        {
            using (FileTraceEventDataReader reader = new FileTraceEventDataReader(ConnectionInfo.ConnectionString, Filter, Events))
            {
                reader.ReadEvents();
            }
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
                                evt.CPU = (long?)Convert.ToInt64(reader.GetValue("CPU")) * 1000; // SqlTrace captures CPU as milliseconds => convert to microseconds
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
                    System.IO.File.Delete(traceFile);
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

    }
}
