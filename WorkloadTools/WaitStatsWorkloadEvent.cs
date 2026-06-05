using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

using ProtoBuf;

namespace WorkloadTools
{
    [ProtoContract]
    public class WaitStatsWorkloadEvent : WorkloadEvent
    {
        [ProtoMember(1)]
        public DataTable Waits;

        public WaitStatsWorkloadEvent()
        {
            Type = EventType.WAIT_stats;
        }

    }
}
