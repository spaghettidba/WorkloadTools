using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools
{
    [Serializable]
    public class ExecutionWorkloadEvent : WorkloadEvent
    {
        public string Text { get; set; }
        public int? SPID { get; set; }
        public string ApplicationName { get; set; }
        public string DatabaseName { get; set; }
        public string LoginName { get; set; }
        public string HostName { get; set; }
        public long? Reads { get; set; }
        public long? Writes { get; set; }
        public long? CPU { get; set; }      // MICROSECONDS
        public long? Duration { get; set; } // MICROSECONDS
        public long? EventSequence { get; set; }
        // This is the requested offset in milliseconds
        // from the the beginning of the workload
        public long ReplayOffset { get; set; } = 0; // MILLISECONDS 
    }
}
