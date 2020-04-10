using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WorkloadTools.Consumer
{
    public abstract class BufferedWorkloadConsumer : WorkloadConsumer
    {
        protected bool stopped = false;
        protected ConcurrentQueue<WorkloadEvent> Buffer { get; set; } = new ConcurrentQueue<WorkloadEvent>();
        protected Task BufferReader { get; set; }

        private SpinWait spin = new SpinWait();

        public int BufferSize { get; set; } = 1000;

        public override sealed void Consume(WorkloadEvent evt)
        {
            if (evt == null)
                return;

            // Ensure that the buffer does not get too big
            while (Buffer.Count >= BufferSize)
            {
                spin.SpinOnce();
            }

            // If the buffer has room, enqueue the event
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

                    spin.SpinOnce();
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
