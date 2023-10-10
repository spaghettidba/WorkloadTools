using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using NLog;

namespace WorkloadTools.Consumer
{
    public abstract class BufferedWorkloadConsumer : WorkloadConsumer
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        protected bool stopped = false;
        protected ConcurrentQueue<WorkloadEvent> Buffer { get; set; } = new ConcurrentQueue<WorkloadEvent>();
        protected Task BufferReader { get; set; }

        private SpinWait spin = new SpinWait();
        
        public int BufferSize { get; set; } = 100000;

        public override sealed void Consume(WorkloadEvent evt)
        {
            if (evt == null)
            {
                return;
            }

            // Ensure that the buffer does not get too big
            while (Buffer.Count >= BufferSize)
            {
                logger.Trace("Buffer is full so spinning");
                spin.SpinOnce();
            }

            // If the buffer has room, enqueue the event
            logger.Trace("Adding event {eventType} with start time {startTime:yyyy-MM-ddTHH\\:mm\\:ss.fffffff} to buffer", evt.Type, evt.StartTime);
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
                WorkloadEvent evt;
                while (!Buffer.TryDequeue(out evt))
                {
                    if (stopped)
                    {
                        return;
                    }

                    spin.SpinOnce();
                }

                if (evt == null)
                {
                    continue;
                }

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
