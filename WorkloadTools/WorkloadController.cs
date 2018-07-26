using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WorkloadTools.Consumer;

namespace WorkloadTools
{
    public class WorkloadController
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        public WorkloadListener Listener { get; set; }
        public List<WorkloadConsumer> Consumers = new List<WorkloadConsumer>();

        private bool stopped = false;
        private bool disposed = false;
        private const int MAX_DISPOSE_TIMEOUT_SECONDS = 5;


        public WorkloadController()
        {
        }

        public void Run()
        {

            try
            {
                Listener.Initialize();
                while (!stopped)
                {
                    if (!Listener.IsRunning)
                        Stop();

                    var evt = Listener.Read();
                    if (evt == null)
                        continue;
                    Parallel.ForEach(Consumers, (cons) =>
                    {
                        cons.Consume(evt);
                    });
                }
                if (!disposed)
                {
                    disposed = true;
                    Listener.Dispose();
                    foreach (var cons in Consumers)
                    {
                        cons.Dispose();
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error("Uncaught Exception");
                logger.Error(e.StackTrace);
            }
        }

        public Task Start()
        {
            return Task.Factory.StartNew(() => Run());
        }

        public void Stop()
        {
            stopped = true;
            int timeout = 0;
            while(!disposed && timeout < (MAX_DISPOSE_TIMEOUT_SECONDS * 1000))
            {
                Thread.Sleep(100);
                timeout += 100;
            }
            if (!disposed)
            {
                disposed = true;
                if(Listener != null)
                    Listener.Dispose();

                foreach (var cons in Consumers)
                {
                    if(cons != null)
                        cons.Dispose();
                }
            }
        }

    }
}
