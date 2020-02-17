using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools.Consumer
{
    public abstract class WorkloadConsumer : IDisposable
    {
        public abstract void Consume(WorkloadEvent evt);

        public abstract bool HasMoreEvents();

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing) { }

    }
}
