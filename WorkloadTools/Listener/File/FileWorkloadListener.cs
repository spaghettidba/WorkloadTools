using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading;
using WorkloadTools.Util;

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


        public FileWorkloadListener() : base()
        {
            Filter = new FileEventFilter();
        }


        public override void Initialize()
        {
            connectionString = "Data Source=" + Source + ";Version=3;Read Only=True;Journal Mode=Off;Synchronous=Off;";

            totalEvents = ValidateFile();
            if (totalEvents < 0)
            {
                throw new FormatException(String.Format("The input file \"{0}\" is not a valid workload file",Source));
            }

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
                filters = "WHERE (" + filters + ") ";

                // these events should not be filtered out
                // 4 - PerformanceCounter
                // 5 - Timeout
                // 6 - WaitStats
                filters += "OR event_type IN (4,5,6)"; 
            }



            try
            {
                string sql = "SELECT * FROM Events " + filters;
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
        // or -1 in case the file format is invalid
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
            WorkloadEvent result = null;
            double msSleep = 0;

            try
            {
                if (reader == null)
                {
                    return null;
                }

                bool validEventFound = false;
               

                do
                {
                    if (!reader.Read())
                    {
                        return null;
                    }
                    result = ReadEvent(reader);

                    if (SynchronizationMode)
                    {
                        if (previousDate != DateTime.MinValue)
                        {
                            msSleep = (result.StartTime - previousDate).TotalMilliseconds;
                            if (msSleep > 0)
                            {
                                if(msSleep > Int32.MaxValue)
                                {
                                    msSleep = Int32.MaxValue;
                                }
                                Thread.Sleep(Convert.ToInt32(msSleep));
                            }
                        }
                    }

                    previousDate = result.StartTime;

                    // Filter events
                    if (result is ExecutionWorkloadEvent)
                    {
                        validEventFound = Filter.Evaluate(result);
                    }
                    else
                    {
                        validEventFound = true;
                    }

                }
                while (!validEventFound);
            }
            catch (Exception e)
            {
                logger.Error($"Unable to read next event. Current event date: {result.StartTime}  Last event date: {previousDate}  Requested sleep: {msSleep}");
                throw;
            }

            return result;
        }


        private WorkloadEvent ReadEvent(SQLiteDataReader reader)
        {
            WorkloadEvent.EventType type = (WorkloadEvent.EventType)reader.GetInt32(reader.GetOrdinal("event_type"));
            int sequence = reader.GetInt32(reader.GetOrdinal("event_sequence"));
            switch (type)
            {
                case WorkloadEvent.EventType.PerformanceCounter:
                    CounterWorkloadEvent cr = new CounterWorkloadEvent();
                    cr.StartTime = reader.GetDateTime(reader.GetOrdinal("start_time"));
                    ReadCounters(sequence, cr);
                    return cr;
                case WorkloadEvent.EventType.WAIT_stats:
                    WaitStatsWorkloadEvent wr = new WaitStatsWorkloadEvent();
                    wr.StartTime = reader.GetDateTime(reader.GetOrdinal("start_time"));
                    wr.Type = type;
                    return wr;
                default:
                    ExecutionWorkloadEvent result = new ExecutionWorkloadEvent();
                    result.ApplicationName = reader.GetString(reader.GetOrdinal("client_app_name"));
                    result.StartTime = reader.GetDateTime(reader.GetOrdinal("start_time"));
                    result.HostName = reader.GetString(reader.GetOrdinal("client_host_name"));
                    result.DatabaseName = reader.GetString(reader.GetOrdinal("database_name"));
                    result.LoginName = reader.GetString(reader.GetOrdinal("server_principal_name"));
                    result.SPID = reader.GetInt32(reader.GetOrdinal("session_id"));
                    result.Text = reader.GetString(reader.GetOrdinal("sql_text"));
                    result.CPU = reader.GetInt32(reader.GetOrdinal("cpu"));
                    result.Duration = reader.GetInt64(reader.GetOrdinal("duration"));
                    result.Reads = reader.GetInt64(reader.GetOrdinal("reads"));
                    result.Writes = reader.GetInt64(reader.GetOrdinal("writes"));
                    result.Type = type;
                    return result;
            }
        }
    

        protected override void Dispose(bool disposing)
        {
            if((reader != null) && (!reader.IsClosed))
            {
                reader.Close();
            }
            conn.Dispose();
        }

        protected override void ReadPerfCountersEvents()
        {
        }

        protected override void ReadWaitStatsEvents()
        {
        }

        private void ReadCounters(int event_sequence, CounterWorkloadEvent cev)
        {
            string sql = "SELECT * FROM Counters WHERE event_sequence = $event_sequence";

            try
            {
                using (SQLiteConnection m_dbConnection = new SQLiteConnection(connectionString))
                {
                    m_dbConnection.Open();
                    try
                    {
                        using (SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection))
                        {
                            command.Parameters.AddWithValue("$event_sequence", event_sequence);
                            using (SQLiteDataReader rdr = command.ExecuteReader())
                            {
                                while (rdr.Read())
                                {
                                    var name = (CounterWorkloadEvent.CounterNameEnum)Enum.Parse(typeof(CounterWorkloadEvent.CounterNameEnum), (string)rdr["name"]);
                                    cev.Counters.Add(name, rdr.GetFloat(rdr.GetOrdinal("value")));
                                }
                                rdr.Close();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        logger.Error(e, String.Format("Unable to query Counters for sequence {0}",event_sequence));
                        throw;
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error(e, "Unable to query Counters from the source file");
            }
        }


        private void ReadWaits(int event_sequence, WaitStatsWorkloadEvent wev)
        {
            string sql = "SELECT * FROM Waits WHERE event_sequence = $event_sequence";

            try
            {
                using (SQLiteConnection m_dbConnection = new SQLiteConnection(connectionString))
                {
                    m_dbConnection.Open();
                    try
                    {
                        DataTable waits = null;

                        using (SQLiteDataAdapter adapter = new SQLiteDataAdapter(sql, conn))
                        {
                            using (DataSet ds = new DataSet())
                            {
                                adapter.Fill(ds);
                                waits = ds.Tables[0];
                            }
                        }

                        var results = from table1 in waits.AsEnumerable()
                                      select new
                              {
                                  wait_type = Convert.ToString(table1["wait_type"]),
                                  wait_sec = Convert.ToDouble(table1["wait_sec"]),
                                  resource_sec = Convert.ToDouble(table1["resource_sec"]),
                                  signal_sec = Convert.ToDouble(table1["signal_sec"]),
                                  wait_count = Convert.ToDouble(table1["wait_count"])
                              };

                        wev.Waits = DataUtils.ToDataTable(results);

                    }
                    catch (Exception e)
                    {
                        logger.Error(e, String.Format("Unable to query Waits for sequence {0}", event_sequence));
                        throw;
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error(e, "Unable to query Waits from the source file");
            }
        }
    }
}
