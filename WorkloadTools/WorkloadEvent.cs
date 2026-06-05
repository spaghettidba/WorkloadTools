using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;

using ProtoBuf;

namespace WorkloadTools
{
    [Serializable]
    [ProtoContract]

    [ProtoInclude(0, typeof(CounterWorkloadEvent))]
    [ProtoInclude(1, typeof(DiskPerfWorkloadEvent))]
    [ProtoInclude(2, typeof(ErrorWorkloadEvent))]
    [ProtoInclude(3, typeof(ExecutionWorkloadEvent))]
    [ProtoInclude(4, typeof(MessageWorkloadEvent))]
    [ProtoInclude(5, typeof(WaitStatsWorkloadEvent))]

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
            DiskPerf = 8,
            Unknown = -1
        }

        [ProtoMember(0)]
        public DateTime StartTime{ get; set; }

        [ProtoMember(1)]
        public EventType Type { get; set; } = EventType.Unknown;
        
    }
}
