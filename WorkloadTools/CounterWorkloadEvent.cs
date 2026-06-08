using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using ProtoBuf;

namespace WorkloadTools
{
    [ProtoContract]
    public class CounterWorkloadEvent : WorkloadEvent
    {
        public enum CounterNameEnum
        {
            AVG_CPU_USAGE = 1
        }

        [ProtoMember(10)]
        public Dictionary<CounterNameEnum, float> Counters { get; internal set; } = new Dictionary<CounterNameEnum, float>();

        public CounterWorkloadEvent()
        {
            Type = EventType.PerformanceCounter;
        }
        
    }
}
