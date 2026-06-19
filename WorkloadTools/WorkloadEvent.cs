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

    [ProtoInclude(10, typeof(CounterWorkloadEvent))]
    [ProtoInclude(11, typeof(DiskPerfWorkloadEvent))]
    [ProtoInclude(12, typeof(ExecutionWorkloadEvent))]
    [ProtoInclude(13, typeof(MessageWorkloadEvent))]
    [ProtoInclude(14, typeof(WaitStatsWorkloadEvent))]

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

        [ProtoMember(1)]
        public DateTime StartTime{ get; set; }

        [ProtoMember(2)]
        public EventType Type { get; set; } = EventType.Unknown;
        
    }
}
