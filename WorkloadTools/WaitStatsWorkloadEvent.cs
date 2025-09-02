using System.Data;

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
