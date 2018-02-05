using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools
{
    public abstract class WorkloadListener : IDisposable
    {
        public SqlConnectionInfo ConnectionInfo { get; set; }
        public string Source { get; set; }
        public WorkloadEventFilter Filter { get; set; }

        public void Dispose() {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected abstract void Dispose(bool disposing);

        public abstract WorkloadEvent Read();

        public abstract void Initialize();
    }
}
