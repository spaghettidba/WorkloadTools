using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools
{
    [Serializable]
    public abstract class WorkloadEvent
    {
        public enum EventType 
        {
            Message = 0,
            RPCCompleted = 1,
            RPCStarting = 2,
            BatchStarting = -3,
            BatchCompleted = 3,
            PerformanceCounter = 4,
            Timeout = 5,
            WAIT_stats = 6,
            Error = 7,
            Unknown = -1
        }

        public DateTime StartTime{ get; set; }
        public EventType Type { get; set; } = EventType.Unknown;
        
    }
}
