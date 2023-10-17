using Microsoft.SqlServer.XEvent.Linq;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using static WorkloadTools.Listener.Trace.TraceEventParser;

namespace WorkloadTools.Listener.ExtendedEvents
{
    public class StreamXEventDataReader : XEventDataReader
    {

        private enum FieldType
        {
            Action,
            Field
        }

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private bool stopped;

        public StreamXEventDataReader(string connectionString, string sessionName, IEventQueue events) : base(connectionString, sessionName, events, ExtendedEventsWorkloadListener.ServerType.AzureSqlDatabase)
        {
        }

        public override void ReadEvents()
        {

            EventCount = 0;
            var transformer = new SqlTransformer();

            using (var eventstream = new QueryableXEventData(
                                            ConnectionString,
                                            SessionName,
                                            EventStreamSourceOptions.EventStream,
                                            EventStreamCacheOptions.CacheToDisk))
            {

                var eventsEnumerator = eventstream.GetEnumerator();

                while (!stopped && eventsEnumerator.MoveNext())
                {
                    var evt = eventsEnumerator.Current;
                    var workloadEvent = new ExecutionWorkloadEvent();
                    try
                    {
                        workloadEvent.EventSequence = Convert.ToInt64(TryGetValue(evt, FieldType.Action, "event_sequence"));
                        var commandText = string.Empty;
                        if (evt.Name == "rpc_starting")
                        {
                            commandText = (string)TryGetValue(evt, FieldType.Field, "statement");
                            workloadEvent.Type = WorkloadEvent.EventType.RPCStarting;
                        }
                        else if (evt.Name == "sql_batch_starting")
                        {
                            commandText = (string)TryGetValue(evt, FieldType.Field, "batch_text");
                            workloadEvent.Type = WorkloadEvent.EventType.BatchStarting;
                        }
                        else if (evt.Name == "rpc_completed")
                        {
                            commandText = (string)TryGetValue(evt, FieldType.Field, "statement");
                            workloadEvent.Type = WorkloadEvent.EventType.RPCCompleted;
                        }
                        else if (evt.Name == "sql_batch_completed")
                        {
                            commandText = (string)TryGetValue(evt, FieldType.Field, "batch_text");
                            workloadEvent.Type = WorkloadEvent.EventType.BatchCompleted;
                        }
                        else if (evt.Name == "login")
                        {
                            var vIsCached = Convert.ToBoolean(TryGetValue(evt, FieldType.Field, "is_cached"));
                            if (!vIsCached) /* If is not cached then consider it a new login */
                            {
                                workloadEvent.Type = WorkloadEvent.EventType.RPCStarting;
                                // A nonpooled login will trigger Login event with EventSubClass = 1
                                // Setting text to sp_reset_connection and including comment on to 
                                // be able to understand this is a nonpooled login on replay
                                commandText = "exec sp_reset_connection /*Nonpooled*/";
                            }
                            else
                            {
                                workloadEvent.Type = WorkloadEvent.EventType.Unknown;
                                continue;
                            }
                        }
                        else if (evt.Name == "attention")
                        {
                            workloadEvent = new ErrorWorkloadEvent();
                            var value = TryGetValue(evt, FieldType.Action, "sql_text");

                            if (value == null)
                            {
                                continue;
                            }

                            try
                            {
                                if (value is string stringValue)
                                {
                                    commandText = stringValue;
                                }
                                else if (value is byte[] byteValue)
                                {
                                    commandText = Encoding.Unicode.GetString(byteValue);
                                }
                                else
                                {
                                    throw new ArgumentException("Argument is of the wrong type");
                                }
                            }
                            catch (Exception e)
                            {
                                logger.Error(e, $"Unable to extract sql_text from attention event. Value is of type ${value.GetType().FullName}");

                            }
                            workloadEvent.Text = commandText;
                            workloadEvent.Type = WorkloadEvent.EventType.Timeout;
                        }
                        else if (evt.Name == "user_event")
                        {
                            workloadEvent = new ErrorWorkloadEvent();
                            var num = (int)TryGetValue(evt, FieldType.Field, "event_id");
                            if (num == 83 || num == 82)
                            {
                                if (TryGetString(evt, FieldType.Field, "user_info").StartsWith("WorkloadTools."))
                                {
                                    commandText = TryGetString(evt, FieldType.Field, "user_data");
                                    workloadEvent.Text = commandText;

                                    if (num == 83)
                                    {
                                        workloadEvent.Type = WorkloadEvent.EventType.Error;
                                    }
                                    else
                                    {
                                        workloadEvent.Type = WorkloadEvent.EventType.Timeout;
                                    }
                                }
                                else
                                {
                                    workloadEvent.Type = WorkloadEvent.EventType.Unknown;
                                    continue;
                                }
                            }
                        }
                        else
                        {
                            workloadEvent.Type = WorkloadEvent.EventType.Unknown;
                            continue;
                        }

                        try
                        {
                            workloadEvent.ApplicationName = TryGetString(evt, FieldType.Action, "client_app_name");
                            workloadEvent.DatabaseName = TryGetString(evt, FieldType.Action, "database_name");
                            workloadEvent.HostName = TryGetString(evt, FieldType.Action, "client_hostname");
                            workloadEvent.LoginName = TryGetString(evt, FieldType.Action, "server_principal_name");
                            workloadEvent.SPID = TryGetInt32(evt, FieldType.Action, "session_id");
                            if (commandText != null)
                            {
                                workloadEvent.Text = commandText;
                            }

                            workloadEvent.StartTime = evt.Timestamp.LocalDateTime;

                            if (workloadEvent.Type == WorkloadEvent.EventType.Error)
                            {
                                workloadEvent.Duration = 0;
                                workloadEvent.CPU = 0;
                            }
                            else if (workloadEvent.Type == WorkloadEvent.EventType.Timeout)
                            {
                                workloadEvent.Duration = TryGetInt64(evt, FieldType.Field, "duration");
                                workloadEvent.CPU = Convert.ToInt64(workloadEvent.Duration);
                            }
                            else
                            {
                                if (evt.Name == "rpc_completed" || evt.Name == "sql_batch_completed")
                                {
                                    workloadEvent.Reads = TryGetInt64(evt, FieldType.Field, "logical_reads");
                                    workloadEvent.Writes = TryGetInt64(evt, FieldType.Field, "writes");
                                    workloadEvent.CPU = TryGetInt64(evt, FieldType.Field, "cpu_time");
                                    workloadEvent.Duration = TryGetInt64(evt, FieldType.Field, "duration");
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            logger.Error(e, "Error converting XE data from the stream.");
                            throw;
                        }

                        // preprocess and filter events
                        if (workloadEvent.Type == WorkloadEvent.EventType.BatchStarting
                            ||
                            workloadEvent.Type == WorkloadEvent.EventType.BatchCompleted
                            ||
                            workloadEvent.Type == WorkloadEvent.EventType.RPCStarting
                            ||
                            workloadEvent.Type == WorkloadEvent.EventType.RPCCompleted
                            ||
                            workloadEvent.Type == WorkloadEvent.EventType.Message)
                        {
                            if (transformer.Skip(workloadEvent.Text))
                            {
                                continue;
                            }

                            workloadEvent.Text = transformer.Transform(workloadEvent.Text);
                        }

                        Events.Enqueue(workloadEvent);

                        EventCount++;
                    }
                    catch (Exception ex)
                    {
                        logger.Error($"Error converting XE data from the stream: {ex.Message}");
                        try
                        {
                            
                            logger.Error($"    event type            : {workloadEvent.Type}");
                            logger.Error($"    client_app_name       : {TryGetString(evt, FieldType.Action, "client_app_name")}");
                            logger.Error($"    database_name         : {TryGetString(evt, FieldType.Action, "database_name")}");
                            logger.Error($"    client_hostname       : {TryGetString(evt, FieldType.Action, "client_hostname")}");
                            logger.Error($"    server_principal_name : {TryGetString(evt, FieldType.Action, "server_principal_name")}");
                            logger.Error($"    session_id            : {TryGetString(evt, FieldType.Action, "session_id")}");
                            logger.Error($"    duration              : {TryGetString(evt, FieldType.Field, "duration")}");
                            logger.Error($"    logical_reads         : {TryGetString(evt, FieldType.Field, "logical_reads")}");
                            logger.Error($"    writes                : {TryGetString(evt, FieldType.Field, "writes")}");
                            logger.Error($"    cpu_time              : {TryGetString(evt, FieldType.Field, "cpu_time")}");
                        }
                        catch (Exception)
                        {
                            //ignore, it is only logging
                        }
                        throw;
                    }
                }
            }
        }

        private object TryGetValue(PublishedEvent evt, FieldType t, string name)
        {
            object result = null;
            if (t == FieldType.Action)
            {
                if (evt.Actions.TryGetValue(name, out var act))
                {
                    result = act.Value;
                }
            }
            else
            {
                if (evt.Fields.TryGetValue(name, out var fld))
                {
                    result = fld.Value;
                }
            }

            // check whether last char is a null char (\0)
            // because this breaks writing this string to the sqlite database
            // which considers it as a BLOB
            if(result is string stringValue)
            {
                while (stringValue.EndsWith("\0"))
                {
                    result = stringValue.Remove(stringValue.Length - 1);
                }
            }
            return result;
        }

        private string TryGetString(PublishedEvent evt, FieldType t, string name)
        {
            var tmp = TryGetValue(evt, t, name);
            if(tmp != null && tmp.GetType() != typeof(DBNull))
            {
                if (tmp is string strinValue)
                {
                    return strinValue;
                }
                else if (tmp is byte[] byteValue)
                {
                    return Encoding.Unicode.GetString(byteValue);
                }
                else
                {
                    throw new ArgumentException("Argument is of the wrong type");
                }
            }
            else
            {
                return null;
            }
        }

        private int? TryGetInt32(PublishedEvent evt, FieldType t, string name)
        {
            var tmp = TryGetValue(evt, t, name);
            if (tmp != null && tmp.GetType() != typeof(DBNull))
            {
                return Convert.ToInt32(tmp);
            }
            else
            {
                return null;
            }
        }

        private long? TryGetInt64(PublishedEvent evt, FieldType t, string name)
        {
            var tmp = TryGetValue(evt, t, name);
            if (tmp != null && tmp.GetType() != typeof(DBNull))
            {
                return Convert.ToInt64(tmp);
            }
            else
            {
                return null;
            }
        }

        public override void Stop()
        {
            stopped = true;
        }
    }
}
