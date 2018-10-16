using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools
{
    public abstract class WorkloadEvent
    {
        public enum EventType 
        {
            RPCCompleted = 1,
            RPCStarted = 2,
            BatchCompleted = 3,
            PerformanceCounter= 4,
            Timeout = 5,
            WAIT_stats = 6,
            Unknown = -1
        }
        public int Id { get; set; }
        public DateTime StartTime{ get; set; }
        public EventType Type { get; set; } = EventType.Unknown;
        
    }
}
