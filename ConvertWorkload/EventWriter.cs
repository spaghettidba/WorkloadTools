using System;
using WorkloadTools;

namespace ConvertWorkload
{
    public abstract class EventWriter : IDisposable
    {
        protected bool stopped;

        public abstract void Write(WorkloadEvent evt);

        public void Dispose()
        {
            stopped = true;
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected abstract void Dispose(bool disposing);

    }
}