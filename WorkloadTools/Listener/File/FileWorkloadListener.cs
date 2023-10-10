using System;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Linq;
using System.Text;

using NLog;

using WorkloadTools.Util;

namespace WorkloadTools.Listener.File
{
    public class FileWorkloadListener : WorkloadListener
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        // Default behaviour is replay events in synchronization mode
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
            var filters = GetFilterClause();

            try
            {
                var sql = string.Empty;

                // Events are executed on event_sequence order
                logger.Info("Reading the full data for every event that matches filters. This may take awhile on large trace files please be patient");
                sql = "SELECT * FROM Events " + filters + " ORDER BY start_time ASC, row_id ASC";

                conn = new SQLiteConnection(connectionString);
                conn.Open();
                var command = new SQLiteCommand(sql, conn);
                reader = command.ExecuteReader();
            }
            catch (Exception e)
            {
                logger.Error(e);
                throw;
            }
        }

        // returns the number of events to replay, if any
        // returns -1 in case the file format is invalid
        private long ValidateFile()
        {
            // Push Down EventFilters
            var filters = GetFilterClause();

            string sql;
            if (string.IsNullOrEmpty(filters))
            {
                // Only works if you didn't delete rows from the table
                // WorkloadTools doesn't delete anything. If you deleted 
                // rows manually, blame yourself.
                sql = "SELECT MAX(ROWID) FROM Events LIMIT 1";
            }
            else
            {
                // SELECT COUNT(*) can be slow on large traces unless VACUUM is used in
                // Sqlite, but is the only solution when filters are applied.
                // When filters are applied extra indexes may be useful in future,
                // especially on large traces.
                sql = "SELECT COUNT(*) FROM Events";
            }

            long result = -1;

            try
            {
                using (var m_dbConnection = new SQLiteConnection(connectionString))
                {
                    m_dbConnection.Open();
                    try
                    {
                        var command = new SQLiteCommand(sql, m_dbConnection);
                        result = (long)command.ExecuteScalar();
                    }
                    catch (Exception e)
                    {
                        result = -1;
                        logger.Error(e, "Unable to query the Events table in source file");
                    }
                }

                logger.Info("The source file contains {result} events", result);
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

            // first I need to return the event that
            // contains the total number of events in the file
            // once this is done I can start sending the actual events
            if (!totalEventsMessageSent)
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

                var validEventFound = false;
                var transformer = new SqlTransformer();

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
                    if (result is ExecutionWorkloadEvent execEvent)
                    {
                        if (SynchronizationMode)
                        {
                            if (startTime != DateTime.MinValue)
                            {
                                var commandOffset = (result.StartTime - startTime).TotalMilliseconds;
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

                        // preprocess and filter events
                        if (execEvent.Type == WorkloadEvent.EventType.BatchStarting
                            ||
                            execEvent.Type == WorkloadEvent.EventType.BatchCompleted
                            ||
                            execEvent.Type == WorkloadEvent.EventType.RPCStarting
                            ||
                            execEvent.Type == WorkloadEvent.EventType.RPCCompleted
                            ||
                            execEvent.Type == WorkloadEvent.EventType.Message)
                        {
                            if (transformer.Skip(execEvent.Text))
                            {
                                continue;
                            }

                            execEvent.Text = transformer.Transform(execEvent.Text);
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
                if (stopped)
                {
                    return null;
                }

                DateTime? eventDate = null;
                if (result != null)
                {
                    eventDate = result.StartTime;
                }

                logger.Error(e);
                logger.Error($"Unable to read next event. Current event date: {eventDate}");
                throw;
            }

            return result;
        }

        private WorkloadEvent ReadEvent(SQLiteDataReader reader)
        {
            var type = (WorkloadEvent.EventType)reader.GetInt32(reader.GetOrdinal("event_type"));
            var row_id = reader.GetInt64(reader.GetOrdinal("row_id"));

            try
            {
                switch (type)
                {
                    case WorkloadEvent.EventType.PerformanceCounter:
                        var cr = new CounterWorkloadEvent
                        {
                            StartTime = reader.GetDateTime(reader.GetOrdinal("start_time"))
                        };
                        ReadCounters(row_id, cr);

                        return cr;

                    case WorkloadEvent.EventType.WAIT_stats:
                        return new WaitStatsWorkloadEvent
                        {
                            StartTime = reader.GetDateTime(reader.GetOrdinal("start_time")),
                            Type = type
                        };

                    case WorkloadEvent.EventType.Error:
                        return new ErrorWorkloadEvent
                        {
                            StartTime = reader.GetDateTime(reader.GetOrdinal("start_time")),
                            Type = type,
                            Text = GetString(reader, "sql_text")
                        };

                    default:
                        return new ExecutionWorkloadEvent
                        {
                            EventSequence = GetInt64(reader, "event_sequence"),
                            ApplicationName = GetString(reader, "client_app_name"),
                            StartTime = reader.GetDateTime(reader.GetOrdinal("start_time")),
                            HostName = GetString(reader, "client_host_name"),
                            DatabaseName = GetString(reader, "database_name"),
                            LoginName = GetString(reader, "server_principal_name"),
                            SPID = reader.GetInt32(reader.GetOrdinal("session_id")),
                            Text = GetString(reader, "sql_text"),
                            CPU = GetInt64(reader, "cpu"),
                            Duration = GetInt64(reader, "duration"),
                            Reads = GetInt64(reader, "reads"),
                            Writes = GetInt64(reader, "writes"),
                            Type = type
                        };

                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Invalid data at row_id {row_id}", e);
            }
        }

        private string GetString(SQLiteDataReader reader, string columnName)
        {
            var result = reader[columnName];
            if (result != null)
            {
                if (result.GetType() == typeof(DBNull))
                {
                    result = null;
                }
                else if (result is byte[] v)
                {
                    result = Encoding.Unicode.GetString(v);
                }
            }
            return (string)result;
        }

        private long? GetInt64(SQLiteDataReader reader, string columnName)
        {
            var result = reader[columnName];
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
            if ((reader != null) && (!reader.IsClosed))
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

        private void ReadCounters(long row_id, CounterWorkloadEvent cev)
        {
            var sql = "SELECT * FROM Counters WHERE row_id = $row_id";

            try
            {
                using (var m_dbConnection = new SQLiteConnection(connectionString))
                {
                    m_dbConnection.Open();
                    try
                    {
                        using (var command = new SQLiteCommand(sql, m_dbConnection))
                        {
                            _ = command.Parameters.AddWithValue("$row_id", row_id);
                            using (var rdr = command.ExecuteReader())
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

        private string GetFilterClause()
        {
            // Push Down EventFilters
            var filters = string.Empty;

            var appFilter = Filter.ApplicationFilter.PushDown();
            var dbFilter = Filter.DatabaseFilter.PushDown();
            var hostFilter = Filter.HostFilter.PushDown();
            var loginFilter = Filter.LoginFilter.PushDown();

            if (appFilter != string.Empty)
            {
                filters += ((filters == string.Empty) ? string.Empty : " AND ") + appFilter;
            }
            if (dbFilter != string.Empty)
            {
                filters += ((filters == string.Empty) ? string.Empty : " AND ") + dbFilter;
            }
            if (hostFilter != string.Empty)
            {
                filters += ((filters == string.Empty) ? string.Empty : " AND ") + hostFilter;
            }
            if (loginFilter != string.Empty)
            {
                filters += ((filters == string.Empty) ? string.Empty : " AND ") + loginFilter;
            }

            if (filters != string.Empty)
            {
                filters = "WHERE (" + filters + ") ";

                // these events should not be filtered out
                // 4 - PerformanceCounter
                // 5 - Timeout
                // 6 - WaitStats
                // 7 - Error
                filters += "OR event_type IN (4,5,6,7)";
            }

            return filters;
        }
    }
}
