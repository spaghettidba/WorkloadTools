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
                                            EventStreamCacheOptions.DoNotCache))
            {

                var eventsEnumerator = eventstream.GetEnumerator();

                while (!stopped && eventsEnumerator.MoveNext())
                {
                    PublishedEvent evt = eventsEnumerator.Current;
                    ExecutionWorkloadEvent workloadEvent = new ExecutionWorkloadEvent();
                    try
                    {

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
                                if (((string)TryGetValue(evt, FieldType.Field, "event_id")).StartsWith("WorkloadTools."))
                                {
                                    object value = TryGetValue(evt, FieldType.Field, "user_data");

                                    if (value is string)
                                        commandText = (string)value;
                                    else if (value is byte[])
                                        commandText = Encoding.Unicode.GetString((byte[])value);
                                    else throw new ArgumentException("Argument is of the wrong type");

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
                            workloadEvent.ApplicationName = (string)TryGetValue(evt, FieldType.Action, "client_app_name");
                            workloadEvent.DatabaseName = (string)TryGetValue(evt, FieldType.Action, "database_name");
                            workloadEvent.HostName = (string)TryGetValue(evt, FieldType.Action, "client_hostname");
                            workloadEvent.LoginName = (string)TryGetValue(evt, FieldType.Action, "server_principal_name");
                            object oSession = TryGetValue(evt, FieldType.Action, "session_id");
                            if (oSession != null)
                                workloadEvent.SPID = Convert.ToInt32(oSession);
                            if (commandText != null)
                                workloadEvent.Text = commandText;


                            workloadEvent.StartTime = evt.Timestamp.LocalDateTime;

                            if (workloadEvent.Type == WorkloadEvent.EventType.Error)
                            {
                                // do nothing
                            }
                            else if (workloadEvent.Type == WorkloadEvent.EventType.Timeout)
                            {
                                workloadEvent.Duration = Convert.ToInt64(TryGetValue(evt, FieldType.Field, "duration"));
                                workloadEvent.CPU = Convert.ToInt64(workloadEvent.Duration);
                            }
                            else
                            {
                                workloadEvent.Reads = Convert.ToInt64(TryGetValue(evt, FieldType.Field, "logical_reads"));
                                workloadEvent.Writes = Convert.ToInt64(TryGetValue(evt, FieldType.Field, "writes"));
                                workloadEvent.CPU = Convert.ToInt64(TryGetValue(evt, FieldType.Field, "cpu_time"));
                                workloadEvent.Duration = Convert.ToInt64(TryGetValue(evt, FieldType.Field, "duration"));
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
                            logger.Error($"    client_app_name       : {evt.Actions["client_app_name"].Value}");
                            logger.Error($"    database_name         : {evt.Actions["database_name"].Value}");
                            logger.Error($"    client_hostname       : {evt.Actions["client_hostname"].Value}");
                            logger.Error($"    server_principal_name : {evt.Actions["server_principal_name"].Value}");
                            logger.Error($"    session_id            : {evt.Actions["session_id"].Value}");
                            logger.Error($"    duration              : {evt.Actions["duration"].Value}");
                            logger.Error($"    logical_reads         : {evt.Actions["logical_reads"].Value}");
                            logger.Error($"    writes                : {evt.Actions["writes"].Value}");
                            logger.Error($"    cpu_time              : {evt.Actions["cpu_time"].Value}");
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

        public override void Stop()
        {
            stopped = true;
        }
    }
}
