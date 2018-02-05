using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WorkloadTools.Listener.Trace;

namespace WorkloadTools.Listener
{
    public class SqlTraceWorkloadListener : WorkloadListener
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        private static int DEFAULT_TRACE_SIZE_MB = 10;
        private static string DEFAULT_DATA_SQL = @"
            DECLARE @defaultData nvarchar(4000);

            EXEC master.dbo.xp_instance_regread
	            N'HKEY_LOCAL_MACHINE',
	            N'Software\Microsoft\MSSQLServer\MSSQLServer',
	            N'DefaultData',
	            @defaultData OUTPUT;

            IF @defaultData IS NULL
            BEGIN
	            SELECT @defaultData = REPLACE(physical_name,'master.mdf','') 
	            FROM sys.master_files
	            WHERE name = 'master';
            END

            SELECT @defaultData AS DefaultData;
        ";

        private int traceId = -1;
        private string tracePath;
        private bool stopped;

        private ConcurrentQueue<WorkloadEvent> events = new ConcurrentQueue<WorkloadEvent>();

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

                    tracePath = GetSqlDefaultDataPath(conn);
                    traceSql = String.Format(traceSql, tracePath  + "sqlworkload", DEFAULT_TRACE_SIZE_MB);
                }
                catch (Exception e)
                {
                    throw new ArgumentException("Cannot open the source script to start the sql trace", e);
                }


                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = traceSql;
                traceId = (int)cmd.ExecuteScalar();

                Task.Factory.StartNew(() => ReadEvents());

            }
        }

        private string GetSqlDefaultDataPath(SqlConnection conn)
        {
            using (SqlCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText = DEFAULT_DATA_SQL;
                return (string)cmd.ExecuteScalar();
            }
        }


        public override WorkloadEvent Read()
        {
            WorkloadEvent result = null;
            while (!events.TryDequeue(out result))
            {
                Thread.Sleep(10);
            }
            return result;
        }

        private void ReadEvents()
        {
            try
            {
                while(!stopped)
                {
                    // get first trace rollover file
                    List<string> files = Directory.GetFiles(tracePath, "sqlworkload*.trc").ToList();
                    files.Sort();
                    string traceFile = files.ElementAt(0);

                    using (FileStream fs = new FileStream(traceFile, FileMode.OpenOrCreate, FileAccess.Read))
                    {
                        while (!fs.CanRead)
                        {
                            Thread.Sleep(5);
                        }
                        using (TraceFileWrapper reader = new TraceFileWrapper())
                        {
                            reader.InitializeAsReader(traceFile);

                            while (reader.Read() && !stopped)
                            {
                                try
                                {
                                    WorkloadEvent evt = new WorkloadEvent();

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

                                    events.Enqueue(evt);
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
                    }
                } // while not stopped
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);

                if (ex.InnerException != null)
                    logger.Error(ex.InnerException.Message);
            }

        }

        protected override void Dispose(bool disposing)
        {
            stopped = true;

            using (SqlConnection conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString;
                conn.Open();


                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = String.Format(@"
                    exec sp_trace_setstatus {0}, 0;
                    exec sp_trace_setstatus {0}, 2;
                ", traceId);
                cmd.ExecuteNonQuery();

            }
        }
    }
}
