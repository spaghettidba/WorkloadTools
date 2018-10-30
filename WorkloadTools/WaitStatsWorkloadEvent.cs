using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace WorkloadTools
{
    [Serializable]
    public class WaitStatsWorkloadEvent : WorkloadEvent
    {
        public DataTable Waits;

        public WaitStatsWorkloadEvent()
        {
            Type = EventType.WAIT_stats;
        }

    }
}
