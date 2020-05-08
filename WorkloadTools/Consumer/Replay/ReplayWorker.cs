using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WorkloadTools.Consumer.Analysis;
using WorkloadTools.Listener;

namespace WorkloadTools.Consumer.Replay
{
    class ReplayWorker : IDisposable
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();
        public bool DisplayWorkerStats { get; set; }
        public bool ConsumeResults { get; set; }
        public int QueryTimeoutSeconds { get; set; }
        public int WorkerStatsCommandCount { get; set; }
        public bool MimicApplicationName { get; set; }

        private SqlConnection conn { get; set; }

        public SqlConnectionInfo ConnectionInfo { get; set; }

        public int ReplayIntervalSeconds { get; set; } = 0;
        public bool StopOnError { get; set; } = false;
        public string Name { get; set; }
        public int SPID { get; set; }
        public bool IsRunning { get; private set; } = false;

        public DateTime StartTime { get; set; }

        public Dictionary<string, string> DatabaseMap { get; set; } = new Dictionary<string, string>();

        private Task runner = null;
        private CancellationTokenSource tokenSource;

        public bool HasCommands
        {
            get
            {
                return !Commands.IsEmpty;
            }
        }

        public int QueueLength
        {
            get
            {
                return Commands.Count;
            }
        }

        public DateTime LastCommandTime { get; private set; }

        private long commandCount = 0;
        private long previousCommandCount = 0;
        private DateTime previousCPSComputeTime = DateTime.Now;
        private List<int> commandsPerSecond = new List<int>();

        private ConcurrentQueue<ReplayCommand> Commands = new ConcurrentQueue<ReplayCommand>();

        public bool IsStopped { get { return stopped; } }
        private bool stopped = false;

        private SqlTransformer transformer = new SqlTransformer();

        private Dictionary<int, int> preparedStatements = new Dictionary<int, int>();
        private SpinWait _spinWait = new SpinWait();

        private enum UserErrorType
        {
            Timeout = 82,
            Error = 83
        }

        private void InitializeConnection()
        {
            logger.Trace($"Worker [{Name}] - Connecting to server {ConnectionInfo.ServerName} for replay...");
            ConnectionInfo.DatabaseMap = this.DatabaseMap;
            string connString = BuildConnectionString();
            conn = new SqlConnection(connString);
            conn.Open();
            logger.Trace($"Worker [{Name}] - Connected");
        }

        private string BuildConnectionString()
        {
            string connectionString = ConnectionInfo.ConnectionString + "; max pool size=500"; 
            return connectionString;
        }

        public void Start()
        {
            tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;

            if (runner == null)
                runner = Task.Factory.StartNew(() => { Run(); }, token);
        }


        public void Run()
        {
            IsRunning = true;
            while (!stopped)
            {
                try
                {
                    ExecuteNextCommand();
                }
                catch(Exception e)
                {
                    logger.Error(e.Message);
                    logger.Error(e.StackTrace);
                }

            }
        }



        public void Stop()
        {
            Stop(true);
        }


        public void Stop(bool withLog)
        {
            if(withLog)
                logger.Trace($"Worker [{Name}] - Stopping");

            stopped = true;
            IsRunning = false;
            if(tokenSource != null)
                tokenSource.Cancel();

            if (withLog)
                logger.Trace("Worker [{Name}] - Stopped");
        }


        public void ExecuteNextCommand()
        {
            ReplayCommand cmd = GetNextCommand();
            if (cmd != null)
            {
                ExecuteCommand(cmd);
                commandCount++;
            }
        }


        public ReplayCommand GetNextCommand()
        {
            ReplayCommand result = null;
            while(!Commands.TryDequeue(out result))
            {
                if (stopped)
                    return null;
                
                _spinWait.SpinOnce();
            }
            return result;
        }


        [MethodImpl(MethodImplOptions.Synchronized)]
        public void ExecuteCommand(ReplayCommand command)
        {
            LastCommandTime = DateTime.Now;

            if (conn == null)
            {
                try
                {
                    ConnectionInfo.ApplicationName = "WorkloadTools-ReplayWorker";
                    if (MimicApplicationName)
                    {
                        ConnectionInfo.ApplicationName = command.ApplicationName;
                        if (String.IsNullOrEmpty(ConnectionInfo.ApplicationName))
                        {
                            ConnectionInfo.ApplicationName = "WorkloadTools-ReplayWorker";
                        }
                    }
                    InitializeConnection();
                }
                catch (SqlException se)
                {
                    logger.Error(se.Message);
                    logger.Error($"Worker [{Name}] - Unable to acquire the connection. Quitting the ReplayWorker");
                    return;
                }
            }

            if (conn != null)
            {
                while (conn.State == System.Data.ConnectionState.Connecting)
                {
                    if (stopped)
                        break;

                    Thread.Sleep(5);
                }
            }

            if (conn == null || (conn.State == System.Data.ConnectionState.Closed) || (conn.State == System.Data.ConnectionState.Broken))
            {
                InitializeConnection();
            }


            // Extract the handle from the prepared statement
            NormalizedSqlText nst = transformer.Normalize(command.CommandText);

            if (nst.CommandType == NormalizedSqlText.CommandTypeEnum.SP_RESET_CONNECTION)
            {
                //Stop(false);
                return;
            }
            else if(nst.CommandType == NormalizedSqlText.CommandTypeEnum.SP_PREPARE)
            {
                command.CommandText = nst.NormalizedText;
            }
            else if (nst.CommandType == NormalizedSqlText.CommandTypeEnum.SP_UNPREPARE || nst.CommandType == NormalizedSqlText.CommandTypeEnum.SP_EXECUTE)
            {
                // look up the statement to unprepare in the dictionary
                if (preparedStatements.ContainsKey(nst.Handle))
                {
                    command.CommandText = nst.NormalizedText + " " + preparedStatements[nst.Handle];

                    if (nst.CommandType == NormalizedSqlText.CommandTypeEnum.SP_UNPREPARE)
                        preparedStatements.Remove(nst.Handle);
                }
                else
                    return; // statement not found: better return
            }


            try
            {
                // Try to remap the database according to the database map
                if (DatabaseMap.ContainsKey(command.Database))
                {
                    command.Database = DatabaseMap[command.Database];
                }

                if (conn.Database != command.Database)
                {
                    logger.Trace($"Worker [{Name}] - Changing database to {command.Database} ");
                    conn.ChangeDatabase(command.Database);
                }

                // if the command comes with a replay offset, do it now
                // the offset in milliseconds is set in
                // FileWorkloadListener
                // The other listeners do not set this value, as they
                // already come with the original timing
                if (command.ReplayOffset > 0)
                {
                    // I am using 7 here as an average compensation for sleep
                    // fluctuations due to Windows preemptive scheduling
                    while((DateTime.Now - StartTime).TotalMilliseconds < command.ReplayOffset - 7)
                    {
                        // Thread.Sleep will not sleep exactly 1 millisecond.
                        // It will yield the current thread and put it back 
                        // in the runnable queue once the sleep delay has expired.
                        // This means that the actual sleep time before the 
                        // current thread gains back control can be much higher 
                        // (15 milliseconds or more)
                        // However we do not need to be super precise here: 
                        // each command has a requested offset from the beginning
                        // of the workload and this class does its best to respect it.
                        // If the previous commands take longer in the target environment
                        // the offset cannot be respected and the command will execute
                        // without further waits, but there is no way to recover 
                        // the delay that has built up to that point.
                        Thread.Sleep(1);
                    }
                    
                }

                using (SqlCommand cmd = new SqlCommand(command.CommandText))
                {
                    cmd.Connection = conn;
                    cmd.CommandTimeout = QueryTimeoutSeconds;

                    if (nst.CommandType == NormalizedSqlText.CommandTypeEnum.SP_PREPARE)
                    {
                        if (cmd.CommandText == null)
                            return;
                        int handle = -1;
                        try
                        {
                            object res = cmd.ExecuteScalar();
                            if (res != null)
                            {
                                handle = (int)res;
                                if (!preparedStatements.ContainsKey(nst.Handle))
                                {
                                    preparedStatements.Add(nst.Handle, handle);
                                }
                            }
                        }
                        catch (NullReferenceException)
                        {
                            throw;
                        }
                    }
                    else if (ConsumeResults)
                    {
                        using(SqlDataReader reader = cmd.ExecuteReader())
                        using (ResultSetConsumer consumer = new ResultSetConsumer(reader))
                        {
                            consumer.Consume();
                        }
                    }
                    else
                    {
                        cmd.ExecuteNonQuery();
                    }
                }

                logger.Trace($"Worker [{Name}] - SUCCES - \n{command.CommandText}");
                if (commandCount > 0 && commandCount % WorkerStatsCommandCount == 0)
                {
                    var seconds = (DateTime.Now - previousCPSComputeTime).TotalSeconds;
                    var cps = (commandCount - previousCommandCount) / ((seconds == 0) ? 1 : seconds);
                    previousCPSComputeTime = DateTime.Now;
                    previousCommandCount = commandCount;

                    if (DisplayWorkerStats)
                    {
                        commandsPerSecond.Add((int)cps);
                        cps = commandsPerSecond.Average();

                        logger.Info($"Worker [{Name}] - {commandCount} commands executed.");
                        logger.Info($"Worker [{Name}] - {Commands.Count} commands pending.");
                        logger.Info($"Worker [{Name}] - Last Event Sequence: {command.EventSequence}");
                        logger.Info($"Worker [{Name}] - {(int)cps} commands per second.");
                    }
                }
            }
            catch(SqlException e)
            {
                // handle timeouts
                if (e.Number == -2)
                {
                    RaiseTimeoutEvent(command.CommandText, conn);
                }
                else
                {
                    RaiseErrorEvent(command, e.Message, conn);
                }

                if (StopOnError)
                {
                    logger.Error($"Worker[{Name}] - Sequence[{command.EventSequence}] - Error: \n{command.CommandText}");
                    throw;
                }
                else
                {
                    logger.Trace($"Worker [{Name}] - Sequence[{command.EventSequence}] - Error: {command.CommandText}");
                    logger.Warn($"Worker [{Name}] - Sequence[{command.EventSequence}] - Error: {e.Message}");
                    logger.Trace(e.StackTrace);
                }
            }
            catch (Exception e)
            {
                if (StopOnError)
                {
                    logger.Error($"Worker[{Name}] - Sequence[{command.EventSequence}] - Error: \n{command.CommandText}");
                    throw;
                }
                else
                {
                    logger.Error($"Worker [{Name}] - Sequence[{command.EventSequence}] - Error: {e.Message}");
                    logger.Error(e.StackTrace);
                }
            }
        }

        private void RaiseTimeoutEvent(string commandText, SqlConnection conn)
        {
            RaiseErrorEvent($"WorkloadTools.Timeout[{QueryTimeoutSeconds}]", commandText, UserErrorType.Timeout, conn);
        }


        private void RaiseErrorEvent(ReplayCommand Command, string ErrorMessage, SqlConnection conn)
        {
            string msg = "";
            msg += "DATABASE:" + Environment.NewLine;
            msg += Command.Database + Environment.NewLine;
            msg += "SEQUENCE:" + Environment.NewLine;
            msg += Command.EventSequence + Environment.NewLine;
            msg += "MESSAGE:" + Environment.NewLine;
            msg += ErrorMessage + Environment.NewLine;
            msg += "--------------------" + Environment.NewLine;
            msg += Command.CommandText;

            RaiseErrorEvent("WorkloadTools.Replay",msg,UserErrorType.Error,conn);
        }


        private void RaiseErrorEvent(string info, string message, UserErrorType type, SqlConnection conn)
        {
            // Raise a custom event. Both SqlTrace and Extended Events can capture this event.
            string sql = "EXEC sp_trace_generateevent @eventid = @eventid, @userinfo = @userinfo, @userdata = @userdata;";

            try
            {
                using (SqlCommand cmd = new SqlCommand(sql))
                {
                    cmd.Connection = conn;
                    cmd.Parameters.Add(new SqlParameter("@eventid", System.Data.SqlDbType.Int) { Value = type });
                    cmd.Parameters.Add(new SqlParameter("@userinfo", System.Data.SqlDbType.NVarChar, 128) { Value = info });
                    cmd.Parameters.Add(new SqlParameter("@userdata", System.Data.SqlDbType.VarBinary, 8000) { Value = Encoding.Unicode.GetBytes(message.Substring(0, message.Length > 8000 ? 8000 : message.Length)) });
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                logger.Warn($"Worker[{Name}] - Unable to raise error event. Message: " + ex.Message);
            }
        }

        public void AppendCommand(ReplayCommand cmd)
        {
            Commands.Enqueue(cmd);
        }


        public void AppendCommand(string commandText, string databaseName)
        {
            Commands.Enqueue(new ReplayCommand() { CommandText = commandText, Database = databaseName });
        }


        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void Dispose(bool disposing)
        {
            Stop();
            if (conn != null)
            {
                if (conn.State == System.Data.ConnectionState.Open)
                {
                    try { conn.Close(); } catch (Exception) { /* swallow */ }
                    try { conn.Dispose(); } catch (Exception) { /* swallow */ }
                }
                conn = null;
            }
            if (runner != null)
            {
                while(!(runner.IsCompleted || runner.IsFaulted || runner.IsCanceled))
                {
                    _spinWait.SpinOnce();
                }
                runner.Dispose();
                runner = null;
            }
            if (tokenSource != null)
            {
                tokenSource.Dispose();
                tokenSource = null;
            }
            logger.Trace($"Worker [{Name}] - Disposed");
        }

    }
}

