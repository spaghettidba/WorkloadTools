using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools
{
    public class WaitStatsWorkloadEvent : WorkloadEvent
    {
        public Dictionary<string, float> waits = new Dictionary<string, float>();

        public WaitStatsWorkloadEvent()
        {
            Type = EventType.WAIT_stats;
        }

    }
}
