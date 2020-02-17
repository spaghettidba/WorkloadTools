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
                if(conn == null)
                {
                    InitializeConnection();
                }
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
                if (conn.Database != command.Database)
                {
                    logger.Trace($"Worker [{Name}] - Changing database to {command.Database} ");
                    conn.ChangeDatabase(command.Database);
                }

                // if the command comes with a replay sleep, do it now
                // the amount of milliseconds to sleep is set in
                // FileWorkloadListener
                // The other listeners do not set this value, as they
                // already come with the original timing
                // 
                // Don't remove the IF test: even Sleep(0) can end up
                // sleeping for 10ms or more. Sleep guarantees that
                // the current thread sleeps for AT LEAST the amount
                // of milliseconds set.
                if (command.BeforeSleepMilliseconds > 2)
                {
                    Thread.Sleep(command.BeforeSleepMilliseconds);
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
                    logger.Error($"Worker[{Name}] - Error: \n{command.CommandText}");
                    throw;
                }
                else
                {
                    logger.Trace($"Worker [{Name}] - Error: {command.CommandText}");
                    logger.Warn($"Worker [{Name}] - Error: {e.Message}");
                    logger.Trace(e.StackTrace);
                }
            }
            catch (Exception e)
            {
                if (StopOnError)
                {
                    logger.Error($"Worker[{Name}] - Error: \n{command.CommandText}");
                    throw;
                }
                else
                {
                    logger.Error($"Worker [{Name}] - Error: {e.Message}");
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

            using (SqlCommand cmd = new SqlCommand(sql))
            {
                cmd.Connection = conn;
                cmd.Parameters.AddWithValue("@eventid", type);
                cmd.Parameters.AddWithValue("@userinfo", info);
                cmd.Parameters.AddWithValue("@userdata", Encoding.Unicode.GetBytes(message.Substring(0, message.Length > 8000 ? 8000 : message.Length)));
                cmd.ExecuteNonQuery();
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

