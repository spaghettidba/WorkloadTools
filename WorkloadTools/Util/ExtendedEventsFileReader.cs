using Microsoft.SqlServer.XEvent.Linq;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WorkloadTools.Listener;

namespace WorkloadTools.Util
{
    public class ExtendedEventsFileReader
    {

        private static Logger logger = LogManager.GetCurrentClassLogger();

        private string Path;
        private bool stopped = false;
        private bool finished = false;
        public IEventQueue Events { get; set; }
        private int EventCount = 0;

        public bool HasFinished
        {
            get
            {
                return finished;
            }
        }

        private enum FieldType
        {
            Action,
            Field
        }

        public ExtendedEventsFileReader(string path, IEventQueue events)
        {
            Path = path;
            Events = events;
        }

        public void Stop()
        {
            stopped = true;
        }

        public void ReadEvents()
        {
            EventCount = 0;
            SqlTransformer transformer = new SqlTransformer();

            using (QueryableXEventData eventstream = new QueryableXEventData(Path))
            {

                var eventsEnumerator = eventstream.GetEnumerator();

                while (!stopped && eventsEnumerator.MoveNext())
                {
                    PublishedEvent evt = eventsEnumerator.Current;
                    ExecutionWorkloadEvent evnt = new ExecutionWorkloadEvent();

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
                        commandText = (string)TryGetValue(evt, FieldType.Action, "sql_text");
                        evnt.Type = WorkloadEvent.EventType.Timeout;
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

                        if (evnt.Type == WorkloadEvent.EventType.Timeout)
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

                    if (transformer.Skip(evnt.Text))
                        continue;

                    evnt.Text = transformer.Transform(evnt.Text);

                    Events.Enqueue(evnt);

                    EventCount++;
                }

                finished = true;
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
    }
}
