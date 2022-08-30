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
            RPCStarting = 1,
            RPCCompleted = 2,
            BatchStarting = 3,
            BatchCompleted = 4,
            PerformanceCounter = 5,
            Timeout = 6,
            WAIT_stats = 7,
			Error = 8,
            Unknown = -1
        }

        public DateTime StartTime{ get; set; }
        public EventType Type { get; set; } = EventType.Unknown;
        
    }
}
