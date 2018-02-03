using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WorkloadTools.Listener.Trace;

namespace WorkloadTools.Listener
{
    public class ProfilerWorkloadListener : WorkloadListener
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        private ConcurrentQueue<WorkloadEvent> events = new ConcurrentQueue<WorkloadEvent>();
        private TraceServerWrapper trace;
        private bool stopped = false;

        public string TraceDefinition { get; set; }


        public override void Initialize()
        {
            SqlConnectionInfoWrapper conn = new SqlConnectionInfoWrapper
            {
                ServerName = ConnectionInfo.ServerName,
                DatabaseName = "master"
            };

            if (String.IsNullOrEmpty(ConnectionInfo.UserName))
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
                trace.InitializeAsReader(conn, TraceDefinition);

                Task.Factory.StartNew(() => ReadEvents());

            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);

                if (ex.InnerException != null)
                    logger.Error(ex.InnerException.Message);
            }
        }



        public override WorkloadEvent Read()
        {
            WorkloadEvent result = null;
            while(!events.TryDequeue(out result)) {
                Thread.Sleep(10);
            }
            return result;
        }


        protected override void Dispose(bool disposing)
        {
            // close the trace, if open
            // shut down the reader thread
            stopped = true;
            try
            {
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
                        WorkloadEvent evt = new WorkloadEvent();

                        if (trace.GetValue("EventClass").ToString() == "RPC:Completed")
                            evt.Type = WorkloadEvent.EventType.RPCCompleted;
                        else if (trace.GetValue("EventClass").ToString() == "SQL:BatchCompleted")
                            evt.Type = WorkloadEvent.EventType.BatchCompleted;
                        else
                            evt.Type = WorkloadEvent.EventType.Unknown;
                        evt.ApplicationName = trace.GetValue("ApplicationName").ToString();
                        evt.DatabaseName = trace.GetValue("DatabaseName").ToString();
                        evt.HostName = trace.GetValue("HostName").ToString();
                        evt.LoginName = trace.GetValue("LoginName").ToString();
                        evt.SPID = Convert.ToInt32(trace.GetValue("SPID"));
                        evt.Text = trace.GetValue("TextData").ToString();
                        evt.Reads = (long)trace.GetValue("Reads");
                        evt.Writes = (long)trace.GetValue("Writes");
                        evt.CPU = (long)trace.GetValue("CPU");
                        evt.Duration = (long)trace.GetValue("Duration");

                        if (!Filter.Evaluate(evt))
                            continue;

                        events.Enqueue(evt);
                    }
                    catch (Exception ex)
                    {
                        logger.Error(ex.Message);

                        if (ex.InnerException != null)
                            logger.Error(ex.InnerException.Message);
                    }


                } // while (Read)

            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);

                if (ex.InnerException != null)
                    logger.Error(ex.InnerException.Message);
            }
        }

}
}

