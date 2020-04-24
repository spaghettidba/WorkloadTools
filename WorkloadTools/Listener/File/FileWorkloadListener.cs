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

        private DateTime startTime = DateTime.MinValue;
        private long totalEvents;
        private SQLiteConnection conn;
        private SQLiteDataReader reader;
        private string connectionString;

        private MessageWorkloadEvent totalEventsMessage = null;
        private bool totalEventsMessageSent = false;

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
                throw new FormatException($"The input file \"{Source}\" is not a valid workload file");
            }

            totalEventsMessage = new MessageWorkloadEvent()
            {
                MsgType = MessageWorkloadEvent.MessageType.TotalEvents,
                Value = totalEvents
            };

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
				// 7 - Error
                filters += "OR event_type IN (4,5,6,7)"; 
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


        // returns the number of events to replay, if any
        // returns -1 in case the file format is invalid
        private long ValidateFile()
        {
            string sql = "SELECT COUNT(*) FROM Events";
            long result = -1;

            try
            {
                using (SQLiteConnection m_dbConnection = new SQLiteConnection(connectionString))
                {
                    m_dbConnection.Open();
                    try
                    {
                        SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
                        result = (long)command.ExecuteScalar();
                    }
                    catch (Exception e)
                    {
                        result = -1;
                        logger.Error(e, "Unable to query the Events table in source file");
                    }
                }

                logger.Info($"The source file contains {result} events.");
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
            long commandOffset = 0;

            // first I need to return the event that
            // contains the total number of events in the file
            // once this is done I can start sending the actual events
            if(!totalEventsMessageSent)
            {
                totalEventsMessageSent = true;
                return totalEventsMessage;
            }


            // process actual events from the file
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
						stopped = true;
                        return null;
                    }
                    result = ReadEvent(reader);

                    // Handle replay sleep for synchronization mode
                    // The sleep cannot happen here, but it has to 
                    // happen later in the replay workflow, because
                    // it would only delay the insertion in the queue
                    // and it would not separate the events during the replay
                    if (result is ExecutionWorkloadEvent)
                    {
                        ExecutionWorkloadEvent execEvent = result as ExecutionWorkloadEvent;
                        if (SynchronizationMode)
                        {
                            if (startTime != DateTime.MinValue)
                            {
                                commandOffset = (long)((result.StartTime - startTime).TotalMilliseconds);
                                if (commandOffset > 0)
                                {
                                    execEvent.ReplayOffset = commandOffset;
                                }
                            }
                            else
                            {
                                startTime = execEvent.StartTime;
                            }
                        }
                        else
                        {
                            // Leave it at 0. The replay consumer will interpret this
                            // as "do not wait for the requested offset" and will replay
                            // the event without waiting
                            execEvent.ReplayOffset = 0;
                        }
                    }
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
                if (stopped) return null;

                DateTime? eventDate = null;
                if(result != null)
                    eventDate = result.StartTime;

                logger.Error(e);
                logger.Error($"Unable to read next event. Current event date: {eventDate}");
                throw;
            }

            return result;
        }


        private WorkloadEvent ReadEvent(SQLiteDataReader reader)
        {
            WorkloadEvent.EventType type = (WorkloadEvent.EventType)reader.GetInt32(reader.GetOrdinal("event_type"));
            long row_id = reader.GetInt64(reader.GetOrdinal("row_id"));
            try
            {
                switch (type)
                {
                    case WorkloadEvent.EventType.PerformanceCounter:
                        CounterWorkloadEvent cr = new CounterWorkloadEvent();
                        cr.StartTime = reader.GetDateTime(reader.GetOrdinal("start_time"));
                        ReadCounters(row_id, cr);
                        return cr;
                    case WorkloadEvent.EventType.WAIT_stats:
                        WaitStatsWorkloadEvent wr = new WaitStatsWorkloadEvent();
                        wr.StartTime = reader.GetDateTime(reader.GetOrdinal("start_time"));
                        wr.Type = type;
                        return wr;
                    case WorkloadEvent.EventType.Error:
                        ErrorWorkloadEvent er = new ErrorWorkloadEvent();
                        er.StartTime = reader.GetDateTime(reader.GetOrdinal("start_time"));
                        er.Type = type;
                        er.Text = GetString(reader, "sql_text");
                        return er;
                    default:
                        ExecutionWorkloadEvent result = new ExecutionWorkloadEvent();
                        result.EventSequence = GetInt64(reader, "event_sequence");
                        result.ApplicationName = GetString(reader, "client_app_name");
                        result.StartTime = reader.GetDateTime(reader.GetOrdinal("start_time"));
                        result.HostName = GetString(reader, "client_host_name");
                        result.DatabaseName = GetString(reader, "database_name");
                        result.LoginName = GetString(reader, "server_principal_name");
                        result.SPID = reader.GetInt32(reader.GetOrdinal("session_id"));
                        result.Text = GetString(reader, "sql_text");
                        result.CPU = GetInt64(reader, "cpu");
                        result.Duration = GetInt64(reader, "duration");
                        result.Reads = GetInt64(reader, "reads");
                        result.Writes = GetInt64(reader, "writes");
                        result.Type = type;
                        return result;
                }
            }
            catch(Exception e)
            {
                throw new InvalidOperationException($"Invalid data at row_id {row_id}",e);
            }
        }


        private string GetString(SQLiteDataReader reader, string columnName)
        {
            object result = reader[columnName];
            if(result != null)
            {
                if (result.GetType() == typeof(DBNull))
                {
                    result = null;
                }
                else if (result is byte[])
                {
                    result = Encoding.Unicode.GetString((byte[])result);
                }
            }
            return (string)result;
        }
    
		private long? GetInt64(SQLiteDataReader reader, string columnName)
		{
			object result = reader[columnName];
			if (result != null)
			{
				if (result.GetType() == typeof(DBNull))
				{
					result = null;
				}
			}
			return (long?)result;
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

        private void ReadCounters(Int64 row_id, CounterWorkloadEvent cev)
        {
            string sql = "SELECT * FROM Counters WHERE row_id = $row_id";

            try
            {
                using (SQLiteConnection m_dbConnection = new SQLiteConnection(connectionString))
                {
                    m_dbConnection.Open();
                    try
                    {
                        using (SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection))
                        {
                            command.Parameters.AddWithValue("$row_id", row_id);
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
                        logger.Error(e, $"Unable to query Counters for row_id {row_id}");
                        throw;
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error(e, "Unable to query Counters from the source file");
            }
        }


        private void ReadWaits(Int64 row_id, WaitStatsWorkloadEvent wev)
        {
            string sql = "SELECT * FROM Waits WHERE row_id = $row_id";

            try
            {
                using (SQLiteConnection m_dbConnection = new SQLiteConnection(connectionString))
                {
                    m_dbConnection.Open();
                    try
                    {
                        DataTable waits = null;

                        using (SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection))
                        {
                            command.Parameters.AddWithValue("$row_id", row_id);

                            using (SQLiteDataAdapter adapter = new SQLiteDataAdapter(command))
                            {
                                using (DataSet ds = new DataSet())
                                {
                                    adapter.Fill(ds);
                                    waits = ds.Tables[0];
                                }
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
                        logger.Error(e, $"Unable to query Waits for row_id {row_id}");
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
