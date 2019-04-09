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
                    ExecutionWorkloadEvent evnt = new ExecutionWorkloadEvent();
                    try
                    {

                        string commandText = String.Empty;
                        if (evt.Name == "rpc_completed")
                        {
                            commandText = (string)TryGetValue(evt, FieldType.Field, "statement");
                            evnt.Type = WorkloadEvent.EventType.RPCCompleted;
                        }
                        else if (evt.Name == "sql_batch_completed")
                        {
                            commandText = (string)TryGetValue(evt, FieldType.Field, "batch_text");
                            evnt.Type = WorkloadEvent.EventType.BatchCompleted;
                        }
                        else if (evt.Name == "attention")
                        {
                            object value = TryGetValue(evt, FieldType.Action, "sql_text");
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
                            evnt.Type = WorkloadEvent.EventType.Timeout;
                        }
                        else if (evt.Name == "user_event")
                        {
                            int num = (int)TryGetValue(evt, FieldType.Field, "event_id");
                            if (num == 83)
                            {
                                commandText = (string)TryGetValue(evt, FieldType.Field, "user_data");
                                evnt.Type = WorkloadEvent.EventType.Error;
                            }
                        }
                        else
                        {
                            evnt.Type = WorkloadEvent.EventType.Unknown;
                            continue;
                        }

                        try
                        {
                            evnt.ApplicationName = (string)TryGetValue(evt, FieldType.Action, "client_app_name");
                            evnt.DatabaseName = (string)TryGetValue(evt, FieldType.Action, "database_name");
                            evnt.HostName = (string)TryGetValue(evt, FieldType.Action, "client_hostname");
                            evnt.LoginName = (string)TryGetValue(evt, FieldType.Action, "server_principal_name");
                            object oSession = TryGetValue(evt, FieldType.Action, "session_id");
                            if (oSession != null)
                                evnt.SPID = Convert.ToInt32(oSession);
                            if (commandText != null)
                                evnt.Text = commandText;


                            evnt.StartTime = evt.Timestamp.LocalDateTime;

                            if (evnt.Type == WorkloadEvent.EventType.Error)
                            {
                                // do nothing
                            }
                            else if (evnt.Type == WorkloadEvent.EventType.Timeout)
                            {
                                evnt.Duration = Convert.ToInt64(evt.Fields["duration"].Value);
                                evnt.CPU = Convert.ToInt64(evnt.Duration);
                            }
                            else
                            {
                                evnt.Reads = Convert.ToInt64(evt.Fields["logical_reads"].Value);
                                evnt.Writes = Convert.ToInt64(evt.Fields["writes"].Value);
                                evnt.CPU = Convert.ToInt64(evt.Fields["cpu_time"].Value);
                                evnt.Duration = Convert.ToInt64(evt.Fields["duration"].Value);
                            }

                        }
                        catch (Exception e)
                        {
                            logger.Error(e, "Error converting XE data from the stream.");
                            throw;
                        }

                        if (evnt.Type <= WorkloadEvent.EventType.BatchCompleted)
                        {
                            if (transformer.Skip(evnt.Text))
                                continue;

                            evnt.Text = transformer.Transform(evnt.Text);
                        }

                        Events.Enqueue(evnt);

                        EventCount++;
                    }
                    catch (Exception ex)
                    {
                        logger.Error(ex, "Error converting XE data from the stream.");
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
            return result;
        }

        public override void Stop()
        {
            stopped = true;
        }
    }
}
