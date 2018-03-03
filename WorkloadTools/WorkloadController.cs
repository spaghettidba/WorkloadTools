using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadTools
{
    public class WorkloadController
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        private WorkloadListener listener;
        private List<WorkloadConsumer> consumers = new List<WorkloadConsumer>();
        private bool stopped = false;


        public WorkloadController(WorkloadListener listener)
        {
            this.listener = listener;
        }

        public void RegisterConsumer(WorkloadConsumer consumer)
        {
            consumers.Add(consumer);
        }


        private void Run()
        {
            listener.Initialize();
            while (!stopped)
            {
                var evt = listener.Read();
                foreach(var cons in consumers)
                {
                    cons.Consume(evt);
                }
            }
            listener.Dispose();
            foreach (var cons in consumers)
            {
                cons.Dispose();
            }
        }

        public Task Start()
        {
            return Task.Factory.StartNew(() => Run());
        }

        public void Stop()
        {
            stopped = true;
        }

    }
}
