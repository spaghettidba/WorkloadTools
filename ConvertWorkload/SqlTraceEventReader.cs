using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WorkloadTools;
using WorkloadTools.Listener;
using WorkloadTools.Listener.Trace;

namespace ConvertWorkload
{
    public class SqlTraceEventReader : EventReader
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        private string tracePath;
        private bool started = false;
        private bool finished = false;

        public SqlTraceEventReader(string path)
        {
            Events = new BinarySerializedBufferedEventQueue();
            Events.BufferSize = 10000;
            tracePath = path;
            Filter = new TraceEventFilter();
    }


        private void ReadEventsFromFile()
        {
            try
            {
                // get first trace rollover file
                var parentDir = Directory.GetParent(tracePath);
                var fileName = Path.GetFileNameWithoutExtension(tracePath) + "*" + Path.GetExtension(tracePath);

                List<string> files = Directory.GetFiles(parentDir.FullName, fileName).ToList();
                files.Sort();

                SqlTransformer transformer = new SqlTransformer();
                int rowsRead = 0;

                foreach (string traceFile in files)
                {

                    using (TraceFileWrapper reader = new TraceFileWrapper())
                    {
                        reader.InitializeAsReader(traceFile);

                        Dictionary<string, string> ColumnNames = null;

                        while (reader.Read() && !stopped)
                        {
                            try
                            {
                                if(ColumnNames == null)
                                {
                                    ColumnNames = new Dictionary<string, string>();

                                    string[] colNames = {
                                        "EventClass",
                                        "ApplicationName",
                                        "HostName",
                                        "LoginName",
                                        "SPID",
                                        "TextData",
                                        "StartTime",
                                        "Reads",
                                        "Writes",
                                        "CPU",
                                        "Duration"
                                    };

                                    foreach(var s in colNames)
                                    {
                                        if (reader.HasAttribute(s)) ColumnNames.Add(s, s);
                                    }
                                    
                                }


                                ExecutionWorkloadEvent evt = new ExecutionWorkloadEvent();

                                if (reader.GetValue("EventClass").ToString() == "RPC:Completed")
                                    evt.Type = WorkloadEvent.EventType.RPCCompleted;
                                else if (reader.GetValue("EventClass").ToString() == "SQL:BatchCompleted")
                                    evt.Type = WorkloadEvent.EventType.BatchCompleted;
                                else
                                {
                                    evt.Type = WorkloadEvent.EventType.Unknown;
                                    continue;
                                }

                                if(ColumnNames.ContainsKey("ApplicationName"))
                                    evt.ApplicationName = (string)reader.GetValue("ApplicationName");
                                if (ColumnNames.ContainsKey("DatabaseName"))
                                    evt.DatabaseName = (string)reader.GetValue("DatabaseName");
                                if (ColumnNames.ContainsKey("HostName"))
                                    evt.HostName = (string)reader.GetValue("HostName");
                                if (ColumnNames.ContainsKey("LoginName"))
                                    evt.LoginName = (string)reader.GetValue("LoginName");
                                if (ColumnNames.ContainsKey("SPID"))
                                    evt.SPID = (int?)reader.GetValue("SPID");
                                if (ColumnNames.ContainsKey("TextData"))
                                    evt.Text = (string)reader.GetValue("TextData");
                                if (ColumnNames.ContainsKey("StartTime"))
                                    evt.StartTime = (DateTime)reader.GetValue("StartTime");

                                if (ColumnNames.ContainsKey("Reads"))
                                    evt.Reads = (long?)reader.GetValue("Reads");
                                if (ColumnNames.ContainsKey("Writes"))
                                    evt.Writes = (long?)reader.GetValue("Writes");
                                if (ColumnNames.ContainsKey("CPU"))
                                    evt.CPU = (long?)Convert.ToInt64(reader.GetValue("CPU")) * 1000; // SqlTrace captures CPU as milliseconds => convert to microseconds
                                if (ColumnNames.ContainsKey("Duration"))
                                    evt.Duration = (long?)reader.GetValue("Duration");

                                if (transformer.Skip(evt.Text))
                                    continue;

                                if (!Filter.Evaluate(evt))
                                    continue;

                                evt.Text = transformer.Transform(evt.Text);

                                Events.Enqueue(evt);

                                rowsRead++;
                            }
                            catch (Exception ex)
                            {
                                logger.Error(ex.Message);

                                if (ex.InnerException != null)
                                    logger.Error(ex.InnerException.Message);
                            }


                        } // while (Read)

                    } // using reader
                     
                } // foreach file
                finished = true;
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);

                if (ex.InnerException != null)
                    logger.Error(ex.InnerException.Message);

                Dispose();
            }

        }

        public override WorkloadEvent Read()
        {
            if (!started)
            {
                Task t = Task.Factory.StartNew(ReadEventsFromFile);
                started = true;
            }

            WorkloadEvent result = null;
            while (!Events.TryDequeue(out result))
            {
                if (stopped)
                    return null;

                Thread.Sleep(5);
            }
            return result;
        }

        protected override void Dispose(bool disposing) 
        {
        }

        public override bool HasMoreElements()
        {
            return !finished && !stopped && (started ? Events.HasMoreElements() : true);
        }

        public override bool HasFinished()
        {
            return finished && !Events.HasMoreElements();
        }
    }
}
