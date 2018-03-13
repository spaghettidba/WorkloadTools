using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WorkloadTools
{
    public abstract class BufferedWorkloadConsumer : WorkloadConsumer
    {
        protected bool stopped = false;
        protected ConcurrentQueue<WorkloadEvent> Buffer { get; set; } = new ConcurrentQueue<WorkloadEvent>();
        protected Task BufferReader { get; set; }

        public override sealed void Consume(WorkloadEvent evt)
        {
            Buffer.Enqueue(evt);

            if(BufferReader == null)
            {
                BufferReader = Task.Factory.StartNew(() => ProcessBuffer());
            }
        }

        protected void ProcessBuffer()
        {
            while (!stopped)
            {
                WorkloadEvent evt = null;
                while (!Buffer.TryDequeue(out evt))
                {
                    if (stopped)
                        return;

                    Thread.Sleep(10);
                }

                if (evt == null)
                    continue;

                ConsumeBuffered(evt);
            }
        }

        protected override void Dispose(bool disposing)
        {
            stopped = true;
        }

        public abstract void ConsumeBuffered(WorkloadEvent evt);

    }
}
