using NLog;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WorkloadTools.Util;

namespace WorkloadTools.Listener.Trace
{
    public class FileTraceEventDataReader : TraceEventDataReader
    {

        private RingBuffer<ReadIteration> ReadIterations = new RingBuffer<ReadIteration>(10);

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

        private TraceUtils utils;

        public FileTraceEventDataReader(string connectionString, WorkloadEventFilter filter, IEventQueue events) : base(connectionString, filter, events)
        {
            utils = new TraceUtils();
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
                        if (ReadIterations.Count > 0)
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

                        ReadTraceData(conn, currentIteration);

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

        private ReadIteration InitializeReadIteration(SqlConnection conn, ReadIteration previous)
        {

            string sqlPath = @"
                SELECT path
                FROM sys.traces
                WHERE id = @traceId;
            ";

            ReadIteration currentIteration = null;
            using (SqlCommand cmdPath = conn.CreateCommand())
            {
                cmdPath.CommandText = sqlPath;

                // Get trace id
                if (traceId == -1)
                {
                    string tracePath = utils.GetSqlDefaultLogPath(conn);
                    traceId = utils.GetTraceId(conn, Path.Combine(tracePath, "sqlworkload"));
                    if (traceId == -1)
                    {
                        throw new InvalidOperationException("The SqlWorkload capture trace is not running.");
                    }
                }
                var paramTraceId = cmdPath.Parameters.Add("@traceId", System.Data.SqlDbType.Int);
                paramTraceId.Value = traceId;

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
                                StartFileName = reader.GetString(0)
                            };
                            currentIteration.EndFileName = currentIteration.StartFileName;
                            if (previous != null)
                            {
                                //if we have a previous iteration, keep reading from that file first
                                currentIteration.StartFileName = previous.EndFileName;
                                // if the file has changed from the previous iteration
                                // read the default number of files ( = 0 )
                                if(currentIteration.StartFileName != currentIteration.EndFileName)
                                {
                                    currentIteration.Files = 0;
                                }

                                // we will use the previous event sequence as the boundary to where 
                                // we need to start reading events again
                                currentIteration.StartSequence = previous.EndSequence;
                                currentIteration.EndSequence = previous.EndSequence;

                                // trace files do not have an offset like xe files but
                                // the offset can be used to go back and read events
                                // from the previous sequence minus a safety offset
                                currentIteration.StartOffset = previous.EndSequence - ReadIteration.TRACE_DEFAULT_OFFSET;
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

        private void ReadTraceData(SqlConnection conn, ReadIteration currentIteration)
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
                FROM fn_trace_gettable(@path, @number_files)
            ";

            if (currentIteration.StartSequence > 0)
            {
                sqlReadTrace += "WHERE EventSequence > @event_offset";
            }

            logger.Debug("Reading Trace data...");

            using (SqlCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText = sqlReadTrace;

                var paramPath = cmd.Parameters.Add("@path", System.Data.SqlDbType.NVarChar, 260);
                paramPath.Value = currentIteration.StartFileName;

                var paramNumberFiles = cmd.Parameters.Add("@number_files", System.Data.SqlDbType.Int);
                paramNumberFiles.Value = currentIteration.Files;

                var paramInitialSequence = cmd.Parameters.Add("@event_offset", System.Data.SqlDbType.BigInt);
                paramInitialSequence.Value = currentIteration.StartOffset;

                // don't pass initial file name and offset
                // read directly from the initial file
                // until we have some rows read already
                if (
                       EventCount == 0
                    || currentIteration.StartOffset <= 0
                    || currentIteration.StartOffset == currentIteration.MinOffset
                )
                {
                    paramPath.Value = currentIteration.StartFileName;
                    paramNumberFiles.Value = 0;
                    paramInitialSequence.Value = 0;
                }

                logger.Debug($"paramPath           : {paramPath.Value}");
                logger.Debug($"paramNumberFiles    : {paramNumberFiles.Value}");
                logger.Debug($"paramInitialSequence: {paramInitialSequence.Value}");

            
                SqlTransformer transformer = new SqlTransformer();

                try
                {
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        int skippedRows = 0;
                        while (reader.Read())
                        {
                            if (reader["EventSequence"] != DBNull.Value)
                                currentIteration.EndSequence = (long)reader["EventSequence"];

                            // read the event from the sqldatareader
                            var evt = parseEvent(reader);

                            // skip invalid events
                            if (evt.Type == WorkloadEvent.EventType.Unknown)
                                continue;

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


                            if (evt.Type <= WorkloadEvent.EventType.BatchCompleted)
                            {
                                if (transformer.Skip(evt.Text))
                                    continue;

                                if (!Filter.Evaluate(evt))
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

                // Wait before querying the events file again
                if (currentIteration.RowsRead < ReadIteration.DEFAULT_TRACE_ROWS_SLEEP_THRESHOLD)
                    Thread.Sleep(ReadIteration.DEFAULT_TRACE_INTERVAL_SECONDS * 1000);

            }

        }

        private ExecutionWorkloadEvent parseEvent(SqlDataReader reader)
        {
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
                return evt;
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

            return evt;
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
