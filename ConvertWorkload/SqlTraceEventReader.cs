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

                        while (reader.Read() && !stopped)
                        {
                            try
                            {
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

                                evt.ApplicationName = (string)reader.GetValue("ApplicationName");
                                evt.DatabaseName = (string)reader.GetValue("DatabaseName");
                                evt.HostName = (string)reader.GetValue("HostName");
                                evt.LoginName = (string)reader.GetValue("LoginName");
                                evt.SPID = (int?)reader.GetValue("SPID");
                                evt.Text = (string)reader.GetValue("TextData");
                                evt.StartTime = (DateTime)reader.GetValue("StartTime");

                                evt.Reads = (long?)reader.GetValue("Reads");
                                evt.Writes = (long?)reader.GetValue("Writes");
                                evt.CPU = (long?)reader.GetValue("CPU") * 1000; // SqlTrace captures CPU as milliseconds => convert to microseconds
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
    }
}
