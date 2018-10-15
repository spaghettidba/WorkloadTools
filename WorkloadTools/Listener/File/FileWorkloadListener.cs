using NLog;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading;

namespace WorkloadTools.Listener.File
{
    public class FileWorkloadListener : WorkloadListener
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        // default behaviour is replay events in synchronization mode
        // (keeping the same event rate found in the source workload).
        // The other option is stress mode: events are replayed one
        // after another without waiting
        public bool SynchronizationMode { get; set; } = true;

        private DateTime startDate = DateTime.MinValue;
        private DateTime previousDate = DateTime.MinValue;
        private int totalEvents;
        private SQLiteConnection conn;
        private SQLiteDataReader reader;
        private string connectionString;

        public FileWorkloadListener()
        {
            Filter = new FileEventFilter();
        }


        public override void Initialize()
        {
            connectionString = "Data Source=" + Source + ";Version=3;";

            totalEvents = ValidateFile();
            if (totalEvents < 0)
            {
                throw new FormatException(String.Format("The input file \"{0}\" is not a valid workload file",Source));
            }

            try
            {
                string sql = "SELECT * FROM Events";
                conn = new SQLiteConnection(connectionString);
                conn.Open();
                SQLiteCommand command = new SQLiteCommand(sql, conn);
                reader = command.ExecuteReader();
            }
            catch(Exception e)
            {
                logger.Error(e);
                throw;
            }
        }


        // returns the number of events to replay 
        // or -1 in case the file format is 
        private int ValidateFile()
        {
            string sql = "SELECT COUNT(*) FROM Events";
            int result = -1;

            try
            {
                using (SQLiteConnection m_dbConnection = new SQLiteConnection(connectionString))
                {
                    m_dbConnection.Open();
                    try
                    {
                        SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
                        result = (int)(long)command.ExecuteScalar();
                    }
                    catch (Exception e)
                    {
                        result = -1;
                        logger.Error(e, "Unable to query the Events table in source file");
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error(e, "Unable to open the source file");
                result = -1;
            }
            return result;
        }

        public override WorkloadEvent Read()
        {
            if(reader == null) 
            {
                return null;
            }

            bool validEventFound = false;
            WorkloadEvent result = null;

            do
            {
                if (!reader.Read())
                {
                    return null;
                }
                result = ReadEvent(reader);

                if (SynchronizationMode)
                {
                    if(previousDate != DateTime.MinValue)
                    {
                        double msSleep = (result.StartTime - previousDate).TotalMilliseconds;
                        Thread.Sleep(Convert.ToInt32(msSleep));
                    }
                }
                
                previousDate = result.StartTime;

                // Filter events
                validEventFound = ApplyFilters(result);

            }
            while (!validEventFound);

            return result;
        }

        private bool ApplyFilters(WorkloadEvent e)
        {
            return Filter.Evaluate(e);
        }

        private WorkloadEvent ReadEvent(SQLiteDataReader reader)
        {
            ExecutionWorkloadEvent result = new ExecutionWorkloadEvent();
            result.ApplicationName = reader.GetString(reader.GetOrdinal("client_app_name"));
            result.StartTime = reader.GetDateTime(reader.GetOrdinal("start_time"));
            result.HostName = reader.GetString(reader.GetOrdinal("client_host_name"));
            result.DatabaseName = reader.GetString(reader.GetOrdinal("database_name"));
            result.LoginName = reader.GetString(reader.GetOrdinal("server_principal_name"));
            result.SPID = reader.GetInt32(reader.GetOrdinal("session_id"));
            result.Text = reader.GetString(reader.GetOrdinal("sql_text"));
            result.Type = (WorkloadEvent.EventType)reader.GetInt32(reader.GetOrdinal("event_type"));
            return result;
        }

        protected override void Dispose(bool disposing)
        {
            if((reader != null) && (!reader.IsClosed))
            {
                reader.Close();
            }
            conn.Dispose();
        }
    }
}
