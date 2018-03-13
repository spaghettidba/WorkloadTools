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
        private static bool COMPUTE_AVERAGE_STATS = Properties.Settings.Default.ReplayWorker_COMPUTE_AVERAGE_STATS;
        private static bool CONSUME_RESULTS = Properties.Settings.Default.ReplayWorker_CONSUME_RESULTS;
        private static int DEFAULT_QUERY_TIMEOUT_SECONDS = Properties.Settings.Default.ReplayWorker_DEFAULT_QUERY_TIMEOUT_SECONDS;
        private static int WORKLOAD_INFO_COMMAND_COUNT = Properties.Settings.Default.ReplayWorker_WORKLOAD_INFO_COMMAND_COUNT;

        private SqlConnection conn { get; set; }

        public SqlConnectionInfo ConnectionInfo { get; set; }

        public int ReplayIntervalSeconds { get; set; } = 0;
        public bool StopOnError { get; set; } = false;
        public string Name { get; set; }
        public int SPID { get; set; }

        private bool isRunning = false;
        public bool IsRunning { get { return isRunning; } }

        public bool HasCommands
        {
            get
            {
                return !Commands.IsEmpty;
            }
        }

        public DateTime LastCommandTime { get; private set; }

        private long commandCount = 0;
        private long previousCommandCount = 0;
        private DateTime previousCPSComputeTime = DateTime.Now;
        private List<int> commandsPerSecond = new List<int>();

        private ConcurrentQueue<ReplayCommand> Commands = new ConcurrentQueue<ReplayCommand>();
        private bool stopped = false;

        private SqlTransformer transformer = new SqlTransformer();

        private Dictionary<int, int> preparedStatements = new Dictionary<int, int>();

        private void InitializeConnection()
        {
            logger.Info(String.Format("Worker [{0}] - Connecting to server {1} for replay...", Name, ConnectionInfo.ServerName));
            string connString = BuildConnectionString();
            conn = new SqlConnection(connString);
            conn.Open();
            logger.Info(String.Format("Worker [{0}] - Connected", Name));
        }

        private string BuildConnectionString()
        {
            ConnectionInfo.ApplicationName = "WorkloadTools-ReplayWorker";
            string connectionString = ConnectionInfo.ConnectionString;
            return connectionString;
        }

        public void Start()
        {
            Task.Factory.StartNew(() => { Run(); });
        }


        public void Run()
        {
            isRunning = true;
            while (!stopped)
            {
                ExecuteNextCommand();
            }
        }


        public void Stop()
        {
            logger.Info(String.Format("Stopping worker [{0}]", Name));
            stopped = true;
            isRunning = false;
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
            while(!stopped && !Commands.TryDequeue(out result))
            {
                Thread.Sleep(10);
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
                    InitializeConnection();
                }
                catch (SqlException se)
                {
                    logger.Error(se.Message);
                    logger.Error(String.Format("Worker [{0}] - Unable to acquire the connection. Quitting the ReplayWorker", Name));
                    return;
                }
            }


            while (!stopped && conn.State == System.Data.ConnectionState.Connecting)
            {
                Thread.Sleep(5);
            }

            if ((conn.State == System.Data.ConnectionState.Closed) || (conn.State == System.Data.ConnectionState.Broken))
            {
                conn.ConnectionString += ";MultipleActiveResultSets=true;";
                conn.Open();
            }


            // Extract the handle from the prepared statement
            NormalizedSqlText nst = transformer.Normalize(command.CommandText);

            if(nst.CommandType == NormalizedSqlText.CommandTypeEnum.SP_PREPARE)
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
                    logger.Trace(String.Format("Worker [{0}] - Changing database to {1} ", Name, command.Database));
                    conn.ChangeDatabase(command.Database);
                }

                using (SqlCommand cmd = new SqlCommand(command.CommandText))
                {
                    cmd.Connection = conn;
                    cmd.CommandTimeout = DEFAULT_QUERY_TIMEOUT_SECONDS;

                    if (nst.CommandType == NormalizedSqlText.CommandTypeEnum.SP_PREPARE)
                    {
                        int handle = (int)cmd.ExecuteScalar();
                        if (!preparedStatements.ContainsKey(nst.Handle))
                        {
                            preparedStatements.Add(nst.Handle, handle);
                        }
                    }
                    else if (CONSUME_RESULTS)
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

                logger.Trace(String.Format("Worker [{0}] - SUCCES - \n{1}", Name, command.CommandText));
                if (commandCount > 0 && commandCount % WORKLOAD_INFO_COMMAND_COUNT == 0)
                {
                    var seconds = (DateTime.Now - previousCPSComputeTime).TotalSeconds;
                    var cps = (commandCount - previousCommandCount) / ((seconds == 0) ? 1 : seconds);
                    previousCPSComputeTime = DateTime.Now;
                    previousCommandCount = commandCount;

                    if (COMPUTE_AVERAGE_STATS)
                    {
                        commandsPerSecond.Add((int)cps);
                        cps = commandsPerSecond.Average();
                    }

                    logger.Info(String.Format("Worker [{0}] - {1} commands executed.", Name, commandCount));
                    logger.Info(String.Format("Worker [{0}] - {1} commands pending.", Name, Commands.Count));
                    logger.Info(String.Format("Worker [{0}] - {1} commands per second.", Name, (int)cps));
                }
            }
            catch (Exception e)
            {
                if (StopOnError)
                {
                    logger.Error(String.Format("Worker[{0}] - Error: \n{1}", Name, command.CommandText));
                    throw;
                }
                else
                {
                    logger.Trace(String.Format("Worker [{0}] - Error: {1}", Name, command.CommandText));
                    logger.Warn(String.Format("Worker [{0}] - Error: {1}", Name, e.Message));
                    logger.Trace(e.StackTrace);
                }
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
        }

    }
}

