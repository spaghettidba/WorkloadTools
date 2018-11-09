using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Xml;
using System.Xml.Linq;

namespace WorkloadTools.Listener.ExtendedEvents
{
    public class FileTargetXEventDataReader : XEventDataReader 
    {

        private static int DEFAULT_TRACE_INTERVAL_SECONDS = Properties.Settings.Default.SqlTraceWorkloadListener_DEFAULT_TRACE_INTERVAL_SECONDS;
        private static int DEFAULT_TRACE_ROWS_SLEEP_THRESHOLD = Properties.Settings.Default.SqlTraceWorkloadListener_DEFAULT_TRACE_ROWS_SLEEP_THRESHOLD;

        private static Logger logger = LogManager.GetCurrentClassLogger();

        private bool stopped = false;

        public FileTargetXEventDataReader(string connectionString, string sessionName, IEventQueue events, ExtendedEventsWorkloadListener.ServerType serverType) : base(connectionString, sessionName, events, serverType)
        {
        }

        public override void ReadEvents()
        {
            string sqlXE = @"
                DECLARE @filename nvarchar(max);

                SELECT @filename = CAST(target_data AS xml).value('(/EventFileTarget/File/@name)[1]','nvarchar(max)') 
                FROM sys.dm_xe_{3}session_targets AS t
                INNER JOIN sys.dm_xe_{3}sessions AS s
	                ON t.event_session_address = s.address
                WHERE s.name = '{2}'
                    AND target_name = 'event_file';

                SELECT timestamp_utc, event_data, file_offset
                FROM sys.fn_xe_file_target_read_file(@filename, NULL, {0}, {1});
            ";

            long lastEvent = -1;

            try
            {
                while (!stopped)
                {
                    using (SqlConnection conn = new SqlConnection())
                    {
                        conn.ConnectionString = ConnectionString;
                        conn.Open();

                        string sql = "";
                        string databaseSuffix = ServerType == ExtendedEventsWorkloadListener.ServerType.AzureSqlDatabase ? "database_" : "";

                        if (lastEvent > 0)
                        {
                            sql = String.Format(sqlXE, "@filename", lastEvent, SessionName, databaseSuffix);
                        }
                        else
                        {
                            sql = String.Format(sqlXE, "NULL", "NULL", SessionName, databaseSuffix);
                        }

                        using (SqlCommand cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = sql;

                            SqlTransformer transformer = new SqlTransformer();

                            int rowsRead = 0;

                            using (SqlDataReader reader = cmd.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    if (reader["file_offset"] != DBNull.Value)
                                        lastEvent = (long)reader["file_offset"];

                                    ExecutionWorkloadEvent evt = new ExecutionWorkloadEvent();

                                    DateTime dt = ((DateTime)reader["timestamp_utc"]);
                                    evt.StartTime = new DateTimeOffset(dt, TimeSpan.Zero).LocalDateTime;

                                    string xmldata = (string)reader["event_data"];

                                    XmlDocument doc = new XmlDocument();
                                    doc.LoadXml(xmldata);

                                    XmlNode eventNode = doc.DocumentElement.SelectSingleNode("/event");
                                    string name = eventNode.Attributes["name"].InnerText;

                                    if(name == "sql_batch_completed")
                                    {
                                        evt.Type = WorkloadEvent.EventType.BatchCompleted;
                                    }
                                    else if (name == "rpc_completed")
                                    {
                                        evt.Type = WorkloadEvent.EventType.RPCCompleted;
                                    }
                                    else if (name == "attention")
                                    {
                                        evt.Type = WorkloadEvent.EventType.Timeout;
                                    }
                                    else
                                    {
                                        evt.Type = WorkloadEvent.EventType.Unknown;
                                        continue;
                                    }

                                    DateTimeOffset timestamp = DateTimeOffset.Parse(eventNode.Attributes["timestamp"].Value);
                                    evt.StartTime = timestamp.LocalDateTime;

                                    foreach (XmlNode node in eventNode.ChildNodes)
                                    {
                                        switch ((string)node.Attributes["name"].Value)
                                        {
                                            case "statement":
                                                if (evt.Type == WorkloadEvent.EventType.RPCCompleted)
                                                {
                                                    evt.Text = (string)node.FirstChild.FirstChild.Value;
                                                }
                                                break;
                                            case "batch_text":
                                                if (evt.Type == WorkloadEvent.EventType.BatchCompleted)
                                                {
                                                    evt.Text = (string)node.FirstChild.FirstChild.Value;
                                                }
                                                break;
                                            case "sql_text":
                                                if (evt.Type == WorkloadEvent.EventType.Timeout)
                                                {
                                                    evt.Text = (string)node.FirstChild.FirstChild.Value;
                                                }
                                                break;
                                            case "client_app_name":
                                                evt.ApplicationName = (string)node.FirstChild.FirstChild.Value;
                                                break;
                                            case "database_name":
                                                evt.DatabaseName = (string)node.FirstChild.FirstChild.Value;
                                                break;
                                            case "client_hostname":
                                                evt.HostName = (string)node.FirstChild.FirstChild.Value;
                                                break;
                                            case "server_principal_name":
                                                evt.LoginName = (string)node.FirstChild.FirstChild.Value;
                                                break;
                                            case "username":
                                                evt.LoginName = (string)node.FirstChild.FirstChild.Value;
                                                break;
                                            case "session_id":
                                                evt.SPID = Convert.ToInt32(node.FirstChild.FirstChild.Value);
                                                break;
                                            case "cpu_time":
                                                evt.CPU = Convert.ToInt32(node.FirstChild.FirstChild.Value);
                                                break;
                                            case "duration":
                                                evt.Duration = Convert.ToInt64(node.FirstChild.FirstChild.Value);
                                                if(evt.Type == WorkloadEvent.EventType.Timeout)
                                                {
                                                    evt.CPU = Convert.ToInt32(evt.Duration / 1000);
                                                }
                                                break;
                                            case "logical_reads":
                                                evt.Reads = Convert.ToInt64(node.FirstChild.FirstChild.Value);
                                                break;
                                            case "writes":
                                                evt.Writes = Convert.ToInt64(node.FirstChild.FirstChild.Value);
                                                break;
                                            default:
                                                break;
                                        }
                                    }


                                    if (transformer.Skip(evt.Text))
                                        continue;

                                    evt.Text = transformer.Transform(evt.Text);

                                    Events.Enqueue(evt);

                                    rowsRead++;
                                    EventCount++;
                                }
                            }

                            // Wait before querying the events file again
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
            }
        }

        public override void Stop()
        {
            stopped = true;
        }
    }
}
