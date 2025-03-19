namespace WorkloadTools
{
    [Serializable]
    public class CounterWorkloadEvent : WorkloadEvent
    {
        public enum CounterNameEnum
        {
            AVG_CPU_USAGE = 1
        }

        public Dictionary<CounterNameEnum, float> Counters { get; internal set; } = new Dictionary<CounterNameEnum, float>();

        public CounterWorkloadEvent()
        {
            Type = EventType.PerformanceCounter;
        }
        
    }
}
