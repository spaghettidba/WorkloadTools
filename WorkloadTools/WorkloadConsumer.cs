using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools
{
    public abstract class WorkloadConsumer
    {
        public abstract void Consume(WorkloadEvent evt);
    }
}
