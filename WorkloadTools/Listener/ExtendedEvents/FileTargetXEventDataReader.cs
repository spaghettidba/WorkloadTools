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
using WorkloadTools.Util;

namespace WorkloadTools.Listener.ExtendedEvents
{
    public class FileTargetXEventDataReader : XEventDataReader, IDisposable
    {
        private RingBuffer<ReadIteration> ReadIterations = new RingBuffer<ReadIteration>(10);

        private static Logger logger = LogManager.GetCurrentClassLogger();

        private bool stopped = false;

        public FileTargetXEventDataReader(string connectionString, string sessionName, IEventQueue events, ExtendedEventsWorkloadListener.ServerType serverType) : base(connectionString, sessionName, events, serverType)
        {
        }

        public override void ReadEvents()
        {
            try
            {
                while (!stopped)
                {
                    using (SqlConnection conn = new SqlConnection())
                    {
                        conn.ConnectionString = ConnectionString;
                        conn.Open();

                        ReadIteration previousIteration = null;
                        if(ReadIterations.Count > 0)
                        {
                            previousIteration = ReadIterations.Last();
                        }
                        var currentIteration = InitializeReadIteration(conn, previousIteration);
                        if (currentIteration != null)
                        {
                            ReadIterations.Add(currentIteration);
                        }
                        else
                        {
                            Stop();
                            break;
                        }

                        ReadXEData(conn, currentIteration);

                        // if reading from localdb one iteration is enough
                        if (ServerType == ExtendedEventsWorkloadListener.ServerType.LocalDB)
                        {
                            break;
                        }

                    }
                }
                logger.Info($"{EventCount} events captured.");
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);
                logger.Error(ex.StackTrace);

                if (ex.InnerException != null)
                    logger.Error(ex.InnerException.Message);
            }
        }



        private void ReadXEData(SqlConnection conn, ReadIteration currentIteration)
        {

            string sqlXE = @"
                SELECT event_data, file_name, file_offset
                FROM sys.fn_xe_file_target_read_file(
                    @filename, 
                    NULL, 
                    @initial_file_name, 
                    @initial_offset
                )
            ";

            logger.Debug("Reading XE data...");

            using (SqlCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText = sqlXE;

                var paramPath = cmd.Parameters.Add("@filename", System.Data.SqlDbType.NVarChar, 260);
                if (ServerType != ExtendedEventsWorkloadListener.ServerType.AzureSqlDatabase)
                {
                    paramPath.Value = currentIteration.GetXEFilePattern();
                }
                else
                {
                    // Azure SqlDatabase does not support wildcards in file names
                    // Specify an exact file name
                    paramPath.Value = currentIteration.StartFileName;
                } 


                var paramInitialFile = cmd.Parameters.Add("@initial_file_name", System.Data.SqlDbType.NVarChar, 260);
                paramInitialFile.Value = currentIteration.StartFileName;

                var paramInitialOffset = cmd.Parameters.Add("@initial_offset", System.Data.SqlDbType.BigInt);
                paramInitialOffset.Value = currentIteration.GetInitialOffset();

                // don't pass initial file name and offset
                // read directly from the initial file
                // until we have some rows read already
                if (
                       EventCount == 0 
                    || currentIteration.StartOffset <=0 
                    || currentIteration.StartOffset == currentIteration.MinOffset
                )
                {
                    if (ServerType != ExtendedEventsWorkloadListener.ServerType.LocalDB)
                    {
                        paramPath.Value = currentIteration.StartFileName;
                    }
                    paramInitialFile.Value = DBNull.Value;
                    paramInitialOffset.Value = DBNull.Value;
                }

                retryWithNULLS:

                logger.Debug($"paramPath         : {paramPath.Value}");
                logger.Debug($"paramInitialFile  : {paramInitialFile.Value}");
                logger.Debug($"paramInitialOffset: {paramInitialOffset.Value}");

                // in case we don't have any data in the xe file
                // GetInitialOffset returns -1 and we need to wait a bit 
                // to let events flow to the file target
                if (currentIteration.GetInitialOffset() > 0)
                {

                    SqlTransformer transformer = new SqlTransformer();

                    using (var reader = cmd.ExecuteReader())
                    {
                        try
                        {
                            int skippedRows = 0;
                            while (reader.Read())
                            {
                                if (reader["file_name"] != DBNull.Value)
                                    currentIteration.EndFileName = (string)reader["file_name"];

                                if (reader["file_offset"] != DBNull.Value)
                                    currentIteration.EndOffset = (long)reader["file_offset"];

                                string xmldata = (string)reader["event_data"];
                                XmlDocument doc = new XmlDocument();
                                doc.LoadXml(xmldata);
                                var evt = parseEvent(doc);

                                // skip to the correct event in case we're reading again
                                // from the same file and we have a reference sequence
                                if ((currentIteration.RowsRead == 0) && (currentIteration.StartSequence > 0))
                                {
                                    // skip rows until we encounter the reference event_sequence
                                    if (evt.EventSequence != currentIteration.StartSequence)
                                    {
                                        skippedRows++;
                                        continue;
                                    }
                                    else
                                    {
                                        // skip one more row...
                                        skippedRows++;
                                        currentIteration.RowsRead++;
                                        continue;
                                    }
                                }


                                // this is only to print out a message, so consider
                                // getting rid of it
                                if (skippedRows > 0)
                                {
                                    logger.Debug($"Skipped rows: {skippedRows}");
                                    skippedRows = 0;
                                }

                                // now we have an event, no matter if good or bad => increment rows read
                                currentIteration.RowsRead++;
                                if (evt.EventSequence != null)
                                {
                                    currentIteration.EndSequence = (long)evt.EventSequence;
                                }

                                if (evt.Type == WorkloadEvent.EventType.Unknown)
                                    continue;

                                if (evt.Type <= WorkloadEvent.EventType.BatchCompleted)
                                {
                                    if (transformer.Skip(evt.Text))
                                        continue;

                                    evt.Text = transformer.Transform(evt.Text);
                                }

                                // it's a "good" event: add it to the queue                       
                                Events.Enqueue(evt);

                                EventCount++;

                            }
                            logger.Debug($"currentIteration.EndSequence : {currentIteration.EndSequence}");


                        }
                        catch (Exception xx)
                        {
                            if (xx.Message.Contains("Specify an offset that exists in the log file"))
                            {
                                // retry the query without specifying the offset / file pair
                                paramInitialFile.Value = DBNull.Value;
                                paramInitialOffset.Value = DBNull.Value;
                                goto retryWithNULLS;
                            }
                            else
                            {
                                throw;
                            }
                        }

                    }
                }

                // Wait before querying the events file again
                if (currentIteration.RowsRead < ReadIteration.DEFAULT_TRACE_ROWS_SLEEP_THRESHOLD)
                    Thread.Sleep(ReadIteration.DEFAULT_TRACE_INTERVAL_SECONDS * 1000);

            }

        }

        private ReadIteration InitializeReadIteration(SqlConnection conn, ReadIteration previous)
        {

            string sqlPath = @"
                SELECT file_name, ISNULL(file_offset,-1) AS file_offset
                FROM (
                    SELECT CAST(target_data AS xml).value('(/EventFileTarget/File/@name)[1]','nvarchar(1000)') AS file_name
                    FROM sys.dm_xe_{0}session_targets AS t
                    INNER JOIN sys.dm_xe_{0}sessions AS s
                        ON t.event_session_address = s.address
                    WHERE s.name = @sessionName
                        AND target_name = 'event_file'
                ) AS fileName
                OUTER APPLY (
                    SELECT TOP(1) file_offset
                    FROM fn_xe_file_target_read_file(file_name,NULL,NULL,NULL)
                ) AS fileOffset;
            ";

            string sqlPathLocaldb = @"
                IF OBJECT_ID('tempdb.dbo.trace_reader_queue') IS NOT NULL
                BEGIN
                    SELECT TOP(1) path, CAST(1 AS bigint) AS file_offset
                    FROM tempdb.dbo.trace_reader_queue
                    ORDER BY ts DESC
                END
                ELSE
                BEGIN
                    SELECT '' AS path, CAST(-1 AS bigint) AS file_offset
                END
            ";

            string databaseSuffix = ServerType == ExtendedEventsWorkloadListener.ServerType.AzureSqlDatabase ? "database_" : "";
            ReadIteration currentIteration = null;
            using (SqlCommand cmdPath = conn.CreateCommand())
            {
                if (ServerType == ExtendedEventsWorkloadListener.ServerType.LocalDB)
                {
                    cmdPath.CommandText = sqlPathLocaldb;
                }
                else
                {
                    cmdPath.CommandText = String.Format(sqlPath, databaseSuffix);
                    var paramSessionName = cmdPath.Parameters.Add("@sessionName", System.Data.SqlDbType.NVarChar, 260);
                    paramSessionName.Value = SessionName;
                }

                try
                {
                    logger.Debug("Initializing read iteration");

                    using (SqlDataReader reader = cmdPath.ExecuteReader())
                    {
                        // should return only one row
                        if (reader.Read())
                        {
                            currentIteration = new ReadIteration()
                            {
                                StartFileName = reader.GetString(0),
                                MinOffset = reader.GetInt64(1)
                            };
                            currentIteration.EndFileName = currentIteration.StartFileName;
                            if (previous != null)
                            {
                                //if we have a previous iteration, keep reading from that file first
                                currentIteration.StartFileName = previous.EndFileName;

                                // we need to read the file from the previous distinct offset
                                // to avoid skipping events. The function fn_xe_file_target_read_file
                                // will skip all events up to the @initial_offset INCLUDED,
                                // so we need to start from the previous offset and skip some rows
                                currentIteration.StartOffset = ReadIteration.GetSecondLastOffset(currentIteration.StartFileName);

                                // we will use the previous event sequence as the boundary to where 
                                // we need to start reading events again
                                currentIteration.StartSequence = previous.EndSequence;
                                currentIteration.EndSequence = previous.EndSequence;

                                // if reading from localdb we don't need to wait for more data
                                if (ServerType == ExtendedEventsWorkloadListener.ServerType.LocalDB)
                                {
                                    if (
                                        (currentIteration.StartFileName == previous.StartFileName) &&
                                        (currentIteration.StartSequence == previous.StartSequence)
                                    )
                                    {
                                        return null;
                                    }
                                }
                            }

                            logger.Debug($"currentIteration.StartFileName: {currentIteration.StartFileName}");
                            logger.Debug($"currentIteration.MinOffset    : {currentIteration.MinOffset}");
                            logger.Debug($"currentIteration.EndFileName  : {currentIteration.EndFileName}");
                            logger.Debug($"currentIteration.StartOffset  : {currentIteration.StartOffset}");
                            logger.Debug($"currentIteration.StartSequence: {currentIteration.StartSequence}");
                        }
                    }
                }
                catch (Exception e)
                {
                    logger.Error(e.StackTrace);
                    throw;
                }
            }
            return currentIteration;
        }


        // Parses all event data from the the data reader
        private ExecutionWorkloadEvent parseEvent(XmlDocument doc)
        {
            ExecutionWorkloadEvent evt = new ExecutionWorkloadEvent();

            XmlNode eventNode = doc.DocumentElement.SelectSingleNode("/event");
            string name = eventNode.Attributes["name"].InnerText;

            if (name == "sql_batch_completed")
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
            else if (name == "user_event")
            {
                evt.Type = WorkloadEvent.EventType.Error;
            }
            else
            {
                evt.Type = WorkloadEvent.EventType.Unknown;
                return evt;
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
                        evt.CPU = Convert.ToInt64(node.FirstChild.FirstChild.Value);
                        break;
                    case "duration":
                        evt.Duration = Convert.ToInt64(node.FirstChild.FirstChild.Value);
                        if (evt.Type == WorkloadEvent.EventType.Timeout)
                        {
                            evt.CPU = Convert.ToInt64(evt.Duration);
                        }
                        break;
                    case "logical_reads":
                        evt.Reads = Convert.ToInt64(node.FirstChild.FirstChild.Value);
                        break;
                    case "writes":
                        evt.Writes = Convert.ToInt64(node.FirstChild.FirstChild.Value);
                        break;
                    case "user_data":
                        evt.Text = (string)node.FirstChild.FirstChild.Value;
                        break;
                    case "event_sequence":
                        evt.EventSequence = Convert.ToInt64(node.FirstChild.FirstChild.Value);

                        break;
                    default:
                        break;
                }
            }
            return evt;
        }

        public override void Stop()
        {
            stopped = true;
        }

        public void Dispose()
        {
            Events.Dispose();
        }
    }

}
