using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools
{
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
        public int? CPU { get; set; }
        public long? Duration { get; set; }

    }
}
