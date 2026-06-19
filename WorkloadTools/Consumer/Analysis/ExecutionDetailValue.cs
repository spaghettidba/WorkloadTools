using System;

namespace WorkloadTools.Consumer.Analysis
{
    public class ExecutionDetailValue
    {
        public DateTime Event_time { get; set; }
        public long? Cpu_us { get; set; }
        public long? Reads { get; set; }
        public long? Writes { get; set; }
        public long? Duration_us { get; set; }
    }
    
}

