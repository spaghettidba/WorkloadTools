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
            RPCCompleted,
            RPCStarted,
            BatchCompleted,
            PerformanceCounter,
            WAIT_stats,
            Unknown
        }

        public DateTime StartTime{ get; set; }
        public EventType Type { get; set; } = EventType.Unknown;

    }
}
