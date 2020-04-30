using Microsoft.SqlServer.XEvent.Linq;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools.Listener.ExtendedEvents
{
    public class StreamXEventDataReader : XEventDataReader
    {

        private enum FieldType
        {
            Action,
            Field
        }

        private static Logger logger = LogManager.GetCurrentClassLogger();

        private bool stopped;

        public StreamXEventDataReader(string connectionString, string sessionName, IEventQueue events) : base(connectionString, sessionName, events, ExtendedEventsWorkloadListener.ServerType.AzureSqlDatabase)
        {
        }


        public override void ReadEvents()
        {

            EventCount = 0;
            SqlTransformer transformer = new SqlTransformer();

            using (QueryableXEventData eventstream = new QueryableXEventData(
                                            ConnectionString,
                                            SessionName,
                                            EventStreamSourceOptions.EventStream,
                                            EventStreamCacheOptions.CacheToDisk))
            {

                var eventsEnumerator = eventstream.GetEnumerator();

                while (!stopped && eventsEnumerator.MoveNext())
                {
                    PublishedEvent evt = eventsEnumerator.Current;
                    ExecutionWorkloadEvent workloadEvent = new ExecutionWorkloadEvent();
                    try
                    {
                        workloadEvent.EventSequence = Convert.ToInt64(TryGetValue(evt, FieldType.Action, "event_sequence"));
                        string commandText = String.Empty;
                        if (evt.Name == "rpc_completed")
                        {
                            commandText = (string)TryGetValue(evt, FieldType.Field, "statement");
                            workloadEvent.Type = WorkloadEvent.EventType.RPCCompleted;
                        }
                        else if (evt.Name == "sql_batch_completed")
                        {
                            commandText = (string)TryGetValue(evt, FieldType.Field, "batch_text");
                            workloadEvent.Type = WorkloadEvent.EventType.BatchCompleted;
                        }
                        else if (evt.Name == "attention")
                        {
                            workloadEvent = new ErrorWorkloadEvent();
                            object value = TryGetValue(evt, FieldType.Action, "sql_text");

                            if (value == null)
                                continue;

                            try
                            {
                                if (value is string)
                                    commandText = (string)value;
                                else if (value is byte[])
                                    commandText = Encoding.Unicode.GetString((byte[])value);
                                else throw new ArgumentException("Argument is of the wrong type");
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
                            int num = (int)TryGetValue(evt, FieldType.Field, "event_id");
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
                                workloadEvent.Text = commandText;


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
                                workloadEvent.Reads = TryGetInt64(evt, FieldType.Field, "logical_reads");
                                workloadEvent.Writes = TryGetInt64(evt, FieldType.Field, "writes");
                                workloadEvent.CPU = TryGetInt64(evt, FieldType.Field, "cpu_time");
                                workloadEvent.Duration = TryGetInt64(evt, FieldType.Field, "duration");
                            }

                        }
                        catch (Exception e)
                        {
                            logger.Error(e, "Error converting XE data from the stream.");
                            throw;
                        }

                        if (workloadEvent.Type <= WorkloadEvent.EventType.BatchCompleted)
                        {
                            if (transformer.Skip(workloadEvent.Text))
                                continue;

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
                PublishedAction act;
                if (evt.Actions.TryGetValue(name, out act))
                {
                    result = act.Value;
                }
            }
            else
            {
                PublishedEventField fld;
                if (evt.Fields.TryGetValue(name, out fld))
                {
                    result = fld.Value;
                }
            }

            // check whether last char is a null char (\0)
            // because this breaks writing this string to the sqlite database
            // which considers it as a BLOB
            if(result is string)
            {
                while (((string)result).EndsWith("\0"))
                {
                    result = ((string)result).Remove(((string)result).Length - 1);
                }
            }
            return result;
        }

        private string TryGetString(PublishedEvent evt, FieldType t, string name)
        {
            object tmp = TryGetValue(evt, t, name);
            if(tmp != null && tmp.GetType() != typeof(DBNull))
            {
                if (tmp is string)
                    return (string)tmp;
                else if (tmp is byte[])
                    return Encoding.Unicode.GetString((byte[])tmp);
                else throw new ArgumentException("Argument is of the wrong type");
            }
            else
            {
                return null;
            }
        }


        private int? TryGetInt32(PublishedEvent evt, FieldType t, string name)
        {
            object tmp = TryGetValue(evt, t, name);
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
            object tmp = TryGetValue(evt, t, name);
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
