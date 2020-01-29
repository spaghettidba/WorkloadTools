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
    public class FileTargetXEventDataReader : XEventDataReader 
    {
        // This class is used internally to keep track
        // of the files, offsets and event_sequences
        private class ReadIteration
        {
            public static long DistinctPreviousOffset { get; private set; } = -1;

            private static long _currentDistinctOffset = -1;
            private static long CurrentDistinctOffset {
                get
                {
                    return _currentDistinctOffset;
                }
                set
                {
                    if(_currentDistinctOffset != value)
                    {
                        // keep track of the previous distinct offset
                        DistinctPreviousOffset = _currentDistinctOffset;
                    }
                    _currentDistinctOffset = value;
                }
            }

            public string StartFileName { get; set; }
            public string EndFileName { get; set; }
            public long MinOffset { get; set; }
            public long StartOffset { get; set; }
            private long _endOffset = -1;
            public long EndOffset {
                get
                {
                    return _endOffset;
                }
                set
                {
                    // set current distinct offset
                    CurrentDistinctOffset = value;

                    // set new value
                    _endOffset = value;
                }
            }
            public long StartSequence { get; set; }
            public long EndSequence { get; set; }
            public long RowsRead { get; set; }
            


            // try to identify the root part of the rollover file name
            // the root is the part of the name before the numeric suffix
            // EG: mySessionName1234.xel => root = mySessionName
            public string GetFilePattern() {
                string filePattern = "";
                for (int j = StartFileName.Length - 4; j > 1 && StartFileName.Substring(j - 1, 1).All(char.IsDigit); j--)
                {
                    filePattern = StartFileName.Substring(0, j - 1);
                }
                filePattern += "*.xel";
                return filePattern;
            }


            // Initial offset to be used as a parameter to the fn_xe_file_target_read_file function
            public long GetInitialOffset()
            {
                long result = -1;
                if (MinOffset > result)
                    result = MinOffset;
                if (StartOffset > result)
                    result = StartOffset;
                if (EndOffset > result)
                    result = EndOffset;
                return result;
            }

            public long GetInitialSequence()
            {
                long result = -1;
                if (StartSequence > result)
                    result = StartSequence;
                if (EndSequence > result)
                    result = EndSequence;
                return result;
            }
        }

        private RingBuffer<ReadIteration> ReadIterations = new RingBuffer<ReadIteration>(10);

        private static int DEFAULT_TRACE_INTERVAL_SECONDS = 10;
        private static int DEFAULT_TRACE_ROWS_SLEEP_THRESHOLD = 5000;

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
                            throw new InvalidOperationException("The current iteration is null, which is not allowed.");
                        }

                        ReadXEData(conn, currentIteration);

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
                paramPath.Value = currentIteration.GetFilePattern();

                var paramInitialFile = cmd.Parameters.Add("@initial_file_name", System.Data.SqlDbType.NVarChar, 260);
                paramInitialFile.Value = currentIteration.StartFileName;

                var paramInitialOffset = cmd.Parameters.Add("@initial_offset", System.Data.SqlDbType.BigInt);
                paramInitialOffset.Value = currentIteration.GetInitialOffset();

                // don't pass initial file name and offset
                // read directly from the initial file
                // until we have some rows read already
                if (EventCount == 0)
                {
                    paramPath.Value = currentIteration.StartFileName;
                    paramInitialFile.Value = DBNull.Value;
                    paramInitialOffset.Value = DBNull.Value;
                }

                logger.Debug($"paramPath         : {paramPath.Value}");
                logger.Debug($"paramInitialFile  : {paramInitialFile.Value}");
                logger.Debug($"paramInitialOffset: {paramInitialOffset.Value}");

                // in case we don't have any data in the xe file
                // GetInitialOffset returns -1 and we need to wait a bit 
                // to let events flow to the file target
                if (currentIteration.GetInitialOffset() > 0)
                {

                    SqlTransformer transformer = new SqlTransformer();

                    try
                    {
                        using (SqlDataReader reader = cmd.ExecuteReader())
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
                                if(skippedRows > 0)
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

                    }
                    catch (Exception)
                    {
                        throw;
                    }
                }

                // Wait before querying the events file again
                if (currentIteration.RowsRead < DEFAULT_TRACE_ROWS_SLEEP_THRESHOLD)
                    Thread.Sleep(DEFAULT_TRACE_INTERVAL_SECONDS * 1000);

            }

        }

        private ReadIteration InitializeReadIteration(SqlConnection conn, ReadIteration previous)
        {

            string sqlPath = @"
                SELECT file_name, ISNULL(file_offset,-1) AS file_offset
                FROM (
                    SELECT CAST(target_data AS xml).value('(/EventFileTarget/File/@name)[1]','nvarchar(1000)') AS file_name
                    FROM sys.dm_xe_session_targets AS t
                    INNER JOIN sys.dm_xe_sessions AS s
                        ON t.event_session_address = s.address
                    WHERE s.name = @sessionName
                        AND target_name = 'event_file'
                ) AS fileName
                OUTER APPLY (
                    SELECT TOP(1) file_offset
                    FROM fn_xe_file_target_read_file(file_name,NULL,NULL,NULL)
                ) AS fileOffset;
            ";

            string databaseSuffix = ServerType == ExtendedEventsWorkloadListener.ServerType.AzureSqlDatabase ? "database_" : "";
            ReadIteration currentIteration = null;
            using (SqlCommand cmdPath = conn.CreateCommand())
            {
                cmdPath.CommandText = String.Format(sqlPath, databaseSuffix);
                var paramSessionName = cmdPath.Parameters.Add("@sessionName", System.Data.SqlDbType.NVarChar, 260);
                paramSessionName.Value = SessionName;

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
                                currentIteration.StartOffset = ReadIteration.DistinctPreviousOffset;

                                // we will use the previous event sequence as the boundary to where 
                                // we need to start reading events again
                                currentIteration.StartSequence = previous.EndSequence;
                                currentIteration.EndSequence = previous.EndSequence;
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
    }

}
