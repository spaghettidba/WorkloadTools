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

        private SpinWait spin = new SpinWait();

        protected XEventDataReader reader;

        public string SessionName { get; set; } = "sqlworkload";

        public enum ServerType
        {
            FullInstance,
            AzureSqlDatabase,
            AzureSqlManagedInstance,
            LocalDB
        }

        private ServerType serverType { get; set; }

        // Path to the file target
        // Mandatory on SqlAzure
        // If not specified, On Premises SQLServer will use the streaming API
        public string FileTargetPath { get; set; }

        private long eventCount;

        public ExtendedEventsWorkloadListener() : base()
        {
            Filter = new ExtendedEventsEventFilter();
            Source = WorkloadController.BaseLocation + "\\Listener\\ExtendedEvents\\sqlworkload.sql";
        }

        public override void Initialize()
        {
            using (SqlConnection conn = new SqlConnection())
            {
                if (ConnectionInfo == null)
                {
                    throw new ArgumentNullException("You need to provide ConnectionInfo to inizialize an ExtendedEventsWorkloadListener");
                }
                conn.ConnectionString = ConnectionInfo.ConnectionString;
                conn.Open();

                LoadServerType(conn);

                if (serverType == ServerType.AzureSqlDatabase)
                {
                    if (FileTargetPath == null)
                    {
                        throw new ArgumentException("Azure SqlDatabase does not support Extended Events streaming. Please specify a path for the FileTarget");
                    }
                    if (ConnectionInfo.DatabaseName == null)
                    {
                        throw new ArgumentException("Azure SqlDatabase does not support starting Extended Events sessions on the master database. Please specify a database name.");
                    }

                    ((ExtendedEventsEventFilter)Filter).IsSqlAzure = true;
                }
                else
                {
                    ConnectionInfo.DatabaseName = "master";
                }

                logger.Info($"Reading Extended Events session definition from {Source}");

                string sessionSql = null;
                try
                {
                    sessionSql = System.IO.File.ReadAllText(Source);

                    // Push Down EventFilters
                    string filters = String.Empty;

                    string appFilter = Filter.ApplicationFilter.PushDown();
                    string dbFilter = Filter.DatabaseFilter.PushDown();
                    string hostFilter = Filter.HostFilter.PushDown();
                    string loginFilter = Filter.LoginFilter.PushDown();

                    if (appFilter != String.Empty)
                    {
                        filters += ((filters == String.Empty) ? String.Empty : " AND ") + appFilter;
                    }
                    if (dbFilter != String.Empty)
                    {
                        filters += ((filters == String.Empty) ? String.Empty : " AND ") + dbFilter;
                    }
                    if (hostFilter != String.Empty)
                    {
                        filters += ((filters == String.Empty) ? String.Empty : " AND ") + hostFilter;
                    }
                    if (loginFilter != String.Empty)
                    {
                        filters += ((filters == String.Empty) ? String.Empty : " AND ") + loginFilter;
                    }

                    if (filters != String.Empty)
                    {
                        filters = "WHERE " + filters;
                    }

                    string sessionType = serverType == ServerType.AzureSqlDatabase ? "DATABASE" : "SERVER";
                    string principalName = serverType == ServerType.AzureSqlDatabase ? "username" : "server_principal_name";

                    sessionSql = String.Format(sessionSql, filters, sessionType, principalName);

                }
                catch (Exception e)
                {
                    throw new ArgumentException("Cannot open the source script to start the extended events session", e);
                }

                StopSession(conn);

                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = sessionSql;
                    cmd.ExecuteNonQuery();
                }
                if (FileTargetPath != null)
                {
                    string sql = @"
                        ALTER EVENT SESSION [{2}] ON {0}
                        ADD TARGET package0.event_file(SET filename=N'{1}',max_file_size=(100))
                    ";

                    sql = String.Format(sql, serverType == ServerType.FullInstance ? "SERVER" : "DATABASE", FileTargetPath, SessionName);

                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = sql;
                        cmd.ExecuteNonQuery();
                    }
                }


                Task.Factory.StartNew(() => ReadEvents());

                //Initialize the source of performance counters events
                Task.Factory.StartNew(() => ReadPerfCountersEvents());

                // Initialize the source of wait stats events
                Task.Factory.StartNew(() => ReadWaitStatsEvents());
            }
        }



        public override WorkloadEvent Read()
        {
            try
            {
                WorkloadEvent result = null;
                while (!Events.TryDequeue(out result))
                {
                    if (stopped)
                        return null;

                    spin.SpinOnce();
                }
                eventCount++;
                return result;
            }
            catch (Exception)
            {
                if (stopped) return null;
                else throw;
            }
        }

        protected override void Dispose(bool disposing)
        {
            stopped = true;
            try
            {
                logger.Info($"Disposing ExtendedEventsWorkloadListener.");
                logger.Debug($"[{eventCount}] events read.");
                logger.Debug($"Events in the queue? [{Events.HasMoreElements()}]");
                reader.Stop();
                using (SqlConnection conn = new SqlConnection())
                {
                    conn.ConnectionString = ConnectionInfo.ConnectionString;
                    conn.Open();
                    StopSession(conn);
                }
            }
            catch (Exception x)
            {
                // swallow
                logger.Warn($"Error disposing ExtendedEventWorkloadListener: {x.Message}");
            }
            logger.Info($"Extended Events session [{SessionName}] stopped successfully.");
        }

        private void StopSession(SqlConnection conn)
        {
            string sql = @"
                DECLARE @condition bit = 0;

                IF SERVERPROPERTY('Edition') = 'SQL Azure'
                    AND SERVERPROPERTY('EngineEdition') = 5
                BEGIN
	                SELECT @condition = 1
	                WHERE EXISTS (
		                SELECT *
		                FROM sys.database_event_sessions
		                WHERE name = '{1}'
	                )
                END
                ELSE
                BEGIN 
	                SELECT @condition = 1
	                WHERE EXISTS (
		                SELECT *
		                FROM sys.server_event_sessions
		                WHERE name = '{1}'
	                )
                END

                IF @condition = 1
                BEGIN
	                BEGIN TRY
		                ALTER EVENT SESSION [{1}] ON {0} STATE = STOP;
	                END TRY
	                BEGIN CATCH
		                -- whoops...
		                PRINT ERROR_MESSAGE()
	                END CATCH
	                BEGIN TRY
		                DROP EVENT SESSION [{1}] ON {0};
	                END TRY
	                BEGIN CATCH
		                -- whoops...
		                PRINT ERROR_MESSAGE()
	                END CATCH
                END
            ";
            sql = String.Format(sql, serverType == ServerType.AzureSqlDatabase ? "DATABASE" : "SERVER", SessionName);
            using (SqlCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }
        }


        private void ReadEvents()
        {
            try
            {

                if (serverType != ServerType.AzureSqlDatabase && FileTargetPath == null)
                {
                    reader = new StreamXEventDataReader(ConnectionInfo.ConnectionString, SessionName, Events);
                }
                else
                {
                    reader = new FileTargetXEventDataReader(ConnectionInfo.ConnectionString, SessionName, Events, serverType);
                }

                reader.ReadEvents();

            }
            catch (Exception ex)
            {
                if (!stopped)
                {
                    logger.Error(ex.Message);
                    logger.Error(ex.StackTrace);

                    if (ex.InnerException != null)
                        logger.Error(ex.InnerException.Message);

                    Dispose();
                }
                else
                {
                    logger.Warn(ex, "The shutdown workflow generated a warning:");
                }
            }
        }





        private void LoadServerType(SqlConnection conn)
        {
            using (SqlCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT SERVERPROPERTY('Edition')";
                string edition = (string)cmd.ExecuteScalar();
                cmd.CommandText = "SELECT SERVERPROPERTY('EngineEdition')";
                int engineEdition = (int)cmd.ExecuteScalar();
                if (edition == "SQL Azure")
                {
                    serverType = ServerType.AzureSqlDatabase;
                    if (engineEdition == 8)
                    {
                        serverType = ServerType.AzureSqlManagedInstance;
                    }
                }
                else
                {
                    serverType = ServerType.FullInstance;
                }
            }
        }
    }
}
