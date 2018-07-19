using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools
{
    public class CounterWorkloadEvent : WorkloadEvent
    {
        public enum CounterNameEnum
        {
            AVG_CPU_USAGE
        }

        public CounterNameEnum Name { get; set; }
        public int Value { get; set; }

        public CounterWorkloadEvent()
        {
            Type = EventType.PerformanceCounter;
        }
        
    }
}
