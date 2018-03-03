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
                trace.InitializeAsReader(conn, Source);

                Task.Factory.StartNew(() => ReadEvents());

            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);

                if (ex.InnerException != null)
                    logger.Error(ex.InnerException.Message);

                throw;
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
            if (stopped) return;
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
                        WorkloadEvent evt = new WorkloadEvent();

                        if (trace.GetValue("EventClass").ToString() == "RPC:Completed")
                            evt.Type = WorkloadEvent.EventType.RPCCompleted;
                        else if (trace.GetValue("EventClass").ToString() == "SQL:BatchCompleted")
                            evt.Type = WorkloadEvent.EventType.BatchCompleted;
                        else
                            evt.Type = WorkloadEvent.EventType.Unknown;
                        evt.ApplicationName = (string)trace.GetValue("ApplicationName");
                        evt.DatabaseName = (string)trace.GetValue("DatabaseName");
                        evt.HostName = (string)trace.GetValue("HostName");
                        evt.LoginName = (string)trace.GetValue("LoginName");
                        evt.SPID = (int?)trace.GetValue("SPID");
                        evt.Text = (string)trace.GetValue("TextData");
                        evt.Reads = (long?)trace.GetValue("Reads");
                        evt.Writes = (long?)trace.GetValue("Writes");
                        evt.CPU = (int?)trace.GetValue("CPU");
                        evt.Duration = (long?)trace.GetValue("Duration");
                        evt.StartTime = DateTime.Now;

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

                Dispose();
            }
        }

}
}

