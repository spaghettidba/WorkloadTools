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

        private enum ServerType
        {
            OnPremises,
            SqlAzure
        }

        private ServerType serverType { get; set; }

        // Path to the file target
        // Mandatory on SqlAzure
        // If not specified, On Premises SQLServer will use the streaming API
        public string FileTargetPath { get; set; }

        public ExtendedEventsWorkloadListener() : base()
        {
            Filter = new ExtendedEventsEventFilter();
            Source = WorkloadController.BaseLocation + "\\Listener\\ExtendedEvents\\sqlworkload.sql";
        }

        public override void Initialize()
        {
            using (SqlConnection conn = new SqlConnection())
            {
                conn.ConnectionString = ConnectionInfo.ConnectionString;
                conn.Open();

                LoadServerType(conn);

                if(serverType == ServerType.SqlAzure)
                {
                    if (FileTargetPath == null)
                    {
                        throw new ArgumentException("Azure SqlDatabase does not support Extended Events streaming. Please specify a path for the FileTarget");
                    }
                    if(ConnectionInfo.DatabaseName == null)
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

                    if(filters != String.Empty)
                    {
                        filters = "WHERE " + filters;
                    }

                    string sessionType = serverType == ServerType.SqlAzure ? "DATABASE" : "SERVER";
                    string principalName = serverType == ServerType.SqlAzure ? "username" : "server_principal_name";

                    sessionSql = String.Format(sessionSql, filters, sessionType, principalName );
                    
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
                        ALTER EVENT SESSION [sqlworkload] ON {0}
                        ADD TARGET package0.event_file(SET filename=N'{1}',max_file_size=(100))
                    ";

                    sql = String.Format(sql, serverType == ServerType.OnPremises ? "SERVER": "DATABASE" , FileTargetPath);

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
            WorkloadEvent result = null;
            while (!Events.TryDequeue(out result))
            {
                if (stopped)
                    return null;

                spin.SpinOnce();
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
                DECLARE @condition bit = 0;

                IF SERVERPROPERTY('Edition') = 'SQL Azure'
                BEGIN
	                SELECT @condition = 1
	                WHERE EXISTS (
		                SELECT *
		                FROM sys.database_event_sessions
		                WHERE name = 'sqlworkload'
	                )
                END
                ELSE
                BEGIN 
	                SELECT @condition = 1
	                WHERE EXISTS (
		                SELECT *
		                FROM sys.server_event_sessions
		                WHERE name = 'sqlworkload'
	                )
                END

                IF @condition = 1
                BEGIN
	                BEGIN TRY
		                ALTER EVENT SESSION [sqlworkload] ON {0} STATE = STOP;
	                END TRY
	                BEGIN CATCH
		                -- whoops...
		                PRINT ERROR_MESSAGE()
	                END CATCH
	                BEGIN TRY
		                DROP EVENT SESSION [sqlworkload] ON {0};
	                END TRY
	                BEGIN CATCH
		                -- whoops...
		                PRINT ERROR_MESSAGE()
	                END CATCH
                END
            ";
            sql = String.Format(sql, serverType == ServerType.OnPremises ? "SERVER" : "DATABASE");
            using (SqlCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }
        }


        private void ReadEvents()
        {
            try {

                XEventDataReader reader;

                if (serverType == ServerType.OnPremises && FileTargetPath == null)
                {
                    reader = new StreamXEventDataReader(ConnectionInfo.ConnectionString, "sqlworkload", Events);
                }
                else
                {
                    reader = new FileTargetXEventDataReader(ConnectionInfo.ConnectionString, "sqlworkload", Events);
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
            string sql = "SELECT SERVERPROPERTY('Edition')";
            using (SqlCommand cmd = conn.CreateCommand())
            {
                cmd.CommandText = sql;
                string edition = (string)cmd.ExecuteScalar();
                if(edition == "SQL Azure")
                {
                    serverType = ServerType.SqlAzure;
                }
                else
                {
                    serverType = ServerType.OnPremises;
                }
            }
        }
    }
}
