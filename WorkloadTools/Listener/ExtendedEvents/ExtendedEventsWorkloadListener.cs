using Microsoft.SqlServer.XEvent.Linq;
using NLog;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WorkloadTools.Listener.ExtendedEvents
{
    public class ExtendedEventsWorkloadListener : WorkloadListener
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        public override void Initialize()
        {
            using (SqlConnection conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString;
                conn.Open();

                string sessionSql = null;
                try
                {
                    sessionSql = File.ReadAllText(Source);

                    // Push Down EventFilters
                    string filters = String.Empty;
                    filters += (filters == String.Empty)?String.Empty:" AND " + Filter.ApplicationFilter.PushDown();
                    filters += (filters == String.Empty)?String.Empty:" AND " + Filter.DatabaseFilter.PushDown();
                    filters += (filters == String.Empty)?String.Empty:" AND " + Filter.HostFilter.PushDown();
                    filters += (filters == String.Empty)?String.Empty:" AND " + Filter.LoginFilter.PushDown();

                    if(filters != String.Empty)
                    {
                        filters = "WHERE " + filters;
                    }

                    String.Format(sessionSql, filters);
                    
                }
                catch (Exception e)
                {
                    throw new ArgumentException("Cannot open the source script to start the extended events session", e);
                }

                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = sessionSql;
                cmd.ExecuteNonQuery();

                ReadEventsFromStream();

                //Initialize the source of performance counters events
                Task.Factory.StartNew(() => ReadPerfCountersEvents());

                // Initialize the source of wait stats events
                Task.Factory.StartNew(() => ReadWaitStatsEvents());
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

        protected override void Dispose(bool disposing)
        {
            stopped = true;
            using (SqlConnection conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString;
                conn.Open();
                StopSession(conn);
            }
            logger.Info("Extended Events session [sqlworkload] stopped successfully.");
        }

        private void StopSession(SqlConnection conn)
        {
            string sql = @"
                IF EXISTS (
	                SELECT *
	                FROM sys.dm_xe_sessions
	                WHERE name = 'sqlworkload'
                )
                BEGIN
                    ALTER EVENT SESSION [sqlworkload] ON SERVER STATE = STOP;
                    DROP EVENT SESSION [sqlworkload] ON SERVER;
                END

                

            ";
            SqlCommand cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }


        private void ReadEventsFromStream()
        {
            try {

                QueryableXEventData eventstream = new QueryableXEventData(
                                ConnectionInfo.ConnectionString,
                                "sqlworkload",
                                EventStreamSourceOptions.EventStream,
                                EventStreamCacheOptions.DoNotCache);

                int rowsRead = 0;
                SqlTransformer transformer = new SqlTransformer();

                foreach (PublishedEvent evt in eventstream)
                {
                    ExecutionWorkloadEvent evnt = new ExecutionWorkloadEvent();

                    string commandText = String.Empty;
                    if (evt.Name == "rpc_completed")
                    {
                        commandText = evt.Fields["statement"].Value.ToString();
                        evnt.Type = WorkloadEvent.EventType.RPCCompleted;
                    }
                    else if (evt.Name == "sql_batch_completed")
                    {
                        commandText = evt.Fields["batch_text"].Value.ToString();
                        evnt.Type = WorkloadEvent.EventType.BatchCompleted;
                    }
                    else if (evt.Name == "attention")
                    {
                        commandText = evt.Fields["sql_text"].Value.ToString();
                        evnt.Type = WorkloadEvent.EventType.Timeout;
                    }
                    else
                    {
                        evnt.Type = WorkloadEvent.EventType.Unknown;
                        continue;
                    }

                    if (evt.Actions["client_app_name"].Value != null)
                        evnt.ApplicationName = (string)evt.Actions["client_app_name"].Value;
                    if (evt.Actions["database_name"].Value != null)
                        evnt.DatabaseName = (string)evt.Actions["database_name"].Value;
                    if (evt.Actions["client_hostname"].Value != null)
                        evnt.HostName = (string)evt.Actions["client_hostname"].Value;
                    if (evt.Actions["server_principal_name"].Value != null)
                        evnt.LoginName = (string)evt.Actions["server_principal_name"].Value;
                    if (evt.Actions["session_id"].Value != null)
                        evnt.SPID = (int)evt.Actions["session_id"].Value;
                    if (commandText != null)
                        evnt.Text = commandText;

                    if (evt.Actions["session_id"].Value != null)
                        evnt.StartTime = (DateTime)evt.Actions["session_id"].Value;

                    evnt.StartTime = evt.Timestamp.DateTime;

                    if (evnt.Type == WorkloadEvent.EventType.Timeout)
                    {
                        evnt.Duration = (int)evt.Fields["duration"].Value;
                        evnt.CPU = Convert.ToInt32(evnt.Duration / 1000);
                    }
                    else
                    {
                        evnt.Reads = (long?)evt.Fields["logical_reads"].Value;
                        evnt.Writes = (long?)evt.Fields["writes"].Value;
                        evnt.CPU = (int?)evt.Fields["cpu_time"].Value;
                        evnt.Duration = (long?)evt.Fields["duration"].Value;
                    }

                    if (transformer.Skip(evnt.Text))
                        continue;

                    evnt.Text = transformer.Transform(evnt.Text);

                    Events.Enqueue(evnt);

                    rowsRead++;


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
    }
}
