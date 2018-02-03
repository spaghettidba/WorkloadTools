using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools
{
    public abstract class BufferedWorkloadConsumer : WorkloadConsumer
    {
        private Queue<WorkloadEvent> Buffer = new Queue<WorkloadEvent>();

        public override void Consume(WorkloadEvent evt)
        {
            Buffer.Enqueue(evt);
        }
    }
}
