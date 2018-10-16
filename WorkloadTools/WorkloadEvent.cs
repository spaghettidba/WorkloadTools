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
            Timeout,
            WAIT_stats,
            Unknown
        }
        public int Id { get; set; }
        public DateTime StartTime{ get; set; }
        public EventType Type { get; set; } = EventType.Unknown;

    }
}
