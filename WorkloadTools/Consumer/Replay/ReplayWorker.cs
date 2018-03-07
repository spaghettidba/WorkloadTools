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

namespace WorkloadTools.Consumer.Replay
{
    class ReplayWorker
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();
        private static bool COMPUTE_AVERAGE_STATS = false;

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

        private long commandCount = 0;
        private long previousCommandCount = 0;
        private DateTime previousCPSComputeTime = DateTime.Now;
        private List<int> commandsPerSecond = new List<int>();

        private ConcurrentQueue<ReplayCommand> Commands = new ConcurrentQueue<ReplayCommand>();
        private bool stopped = false;

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
            while(!Commands.TryDequeue(out result))
            {
                Thread.Sleep(1);
            }
            return result;
        }


        [MethodImpl(MethodImplOptions.Synchronized)]
        public void ExecuteCommand(ReplayCommand command)
        {
            // skip reset connection commands
            if (command.CommandText.Contains("sp_reset_connection"))
                return;

            // skip unprepare commands
            if (command.CommandText.Contains("sp_unprepare "))
                return;

            // skip cursor fetch
            if (command.CommandText.Contains("sp_cursorfetch "))
                return;

            // skip cursor close
            if (command.CommandText.Contains("sp_cursorclose "))
                return;

            // skip sp_execute
            if (command.CommandText.Contains("sp_execute "))
                return;

            // remove the handle from the sp_prepexec call
            if (command.CommandText.Contains("sp_prepexec "))
            {
                int idx = command.CommandText.IndexOf("set @p1=");
                if(idx > 0)
                    command.CommandText = command.CommandText.Insert(idx, @"--");
                command.CommandText += Environment.NewLine + "EXEC sp_unprepare @p1;";
            }

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


            while (conn.State == System.Data.ConnectionState.Connecting)
            {
                Thread.Sleep(5);
            }

            if ((conn.State == System.Data.ConnectionState.Closed) || (conn.State == System.Data.ConnectionState.Broken))
            {
                conn.ConnectionString += ";MultipleActiveResultSets=true;";
                conn.Open();
            }


            // decode declaration
            if (command.CommandText.Contains("sp_cursoropen "))
            {
                command.CommandText = DecodeCursor(command.CommandText, conn);
            }



            try
            {
                if (conn.Database != command.Database)
                {
                    logger.Trace(String.Format("Worker [{0}] - Changing database to {1} ", Name, command.Database));
                    conn.ChangeDatabase(command.Database);
                }

                SqlCommand cmd = new SqlCommand(command.CommandText);
                cmd.Connection = conn;
                cmd.ExecuteNonQuery();

                logger.Trace(String.Format("Worker [{0}] - SUCCES - \n{1}", Name, command.CommandText));
                if (commandCount % 100 == 0)
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


        private string DecodeCursor(string commandText, SqlConnection conn)
        {
            string result = commandText;

            using (SqlCommand cmd = new SqlCommand(commandText.Replace("exec sp_cursoropen ", "SELECT "), conn))
            {
                using (SqlDataReader reader = cmd.ExecuteReader())
                {

                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            result = reader.GetString(1);
                        }
                    }
                }
            }

            return result;

        }

        public void AppendCommand(ReplayCommand cmd)
        {
            Commands.Enqueue(cmd);
        }


        public void AppendCommand(string commandText, string databaseName)
        {
            Commands.Enqueue(new ReplayCommand() { CommandText = commandText, Database = databaseName });
        }

    }
}
