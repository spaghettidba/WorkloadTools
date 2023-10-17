using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WorkloadTools.Listener.Trace;

namespace WorkloadTools.Listener.Trace
{
    public class ProfilerWorkloadListener : WorkloadListener
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly ConcurrentQueue<WorkloadEvent> events = new ConcurrentQueue<WorkloadEvent>();
        private TraceServerWrapper trace;

        public ProfilerWorkloadListener() : base()
        {
            Filter = new ProfilerEventFilter();
            Source = WorkloadController.BaseLocation + "\\Listener\\Trace\\sqlworkload.tdf";
        }
        

        public override void Initialize()
        {
            var conn = new SqlConnectionInfoWrapper
            {
                ServerName = ConnectionInfo.ServerName,
                DatabaseName = "master"
            };

            if (string.IsNullOrEmpty(ConnectionInfo.UserName))
            {
                conn.UseIntegratedSecurity = true;
            }
            else
            {
                conn.UserName = ConnectionInfo.UserName;
                conn.Password = ConnectionInfo.Password;
            }

            trace = new TraceServerWrapper();

            try
            {
                trace.InitializeAsReader(conn, Source);

                _ = Task.Factory.StartNew(() => ReadEvents());

            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);

                if (ex.InnerException != null)
                {
                    logger.Error(ex.InnerException.Message);
                }

                throw;
            }
        }

        public override WorkloadEvent Read()
        {
            WorkloadEvent result = null;
            while(!stopped && !events.TryDequeue(out result)) {
                Thread.Sleep(10);
            }
            return result;
        }

        protected override void Dispose(bool disposing)
        {
            if (stopped)
            {
                return;
            }
            // close the trace, if open
            // shut down the reader thread
            stopped = true;
            try
            {
                trace.Close();
                trace.Stop();
            }
            catch (Exception)
            {
                // naughty dev swallows exceptions...
            }
        }

        private void ReadEvents()
        {
            try
            {
                while (trace.Read() && !stopped)
                {
                    try
                    {
                        var evt = new ExecutionWorkloadEvent();

                        if (trace.GetValue("EventClass").ToString() == "RPC:Completed")
                        {
                            evt.Type = WorkloadEvent.EventType.RPCCompleted;
                        }
                        else if (trace.GetValue("EventClass").ToString() == "SQL:BatchCompleted")
                        {
                            evt.Type = WorkloadEvent.EventType.BatchCompleted;
                        }
                        else
                        {
                            evt.Type = WorkloadEvent.EventType.Unknown;
                        }

                        evt.ApplicationName = (string)trace.GetValue("ApplicationName");
                        evt.DatabaseName = (string)trace.GetValue("DatabaseName");
                        evt.HostName = (string)trace.GetValue("HostName");
                        evt.LoginName = (string)trace.GetValue("LoginName");
                        evt.SPID = (int?)trace.GetValue("SPID");
                        evt.Text = (string)trace.GetValue("TextData");
                        evt.Reads = (long?)trace.GetValue("Reads");
                        evt.Writes = (long?)trace.GetValue("Writes");
                        evt.CPU = (long?)trace.GetValue("CPU") * 1000; // Profiler captures CPU as milliseconds => convert to microseconds
                        evt.Duration = (long?)trace.GetValue("Duration");
                        evt.StartTime = DateTime.Now;

                        if (!Filter.Evaluate(evt))
                        {
                            continue;
                        }

                        events.Enqueue(evt);
                    }
                    catch (Exception ex)
                    {
                        logger.Error(ex.Message);

                        if (ex.InnerException != null)
                        {
                            logger.Error(ex.InnerException.Message);
                        }
                    }

                } // while (Read)

            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);

                if (ex.InnerException != null)
                {
                    logger.Error(ex.InnerException.Message);
                }

                Dispose();
            }
        }

}
}

