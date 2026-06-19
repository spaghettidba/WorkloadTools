using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using ProtoBuf;

namespace WorkloadTools
{
    [ProtoContract]

    [ProtoInclude(100, typeof(ErrorWorkloadEvent))]
    public class ExecutionWorkloadEvent : WorkloadEvent
    {
        [ProtoMember(10)]
        public string Text { get; set; }
        [ProtoMember(11)]
        public int? SPID { get; set; }
        [ProtoMember(12)]
        public string ApplicationName { get; set; }
        [ProtoMember(13)]
        public string DatabaseName { get; set; }
        [ProtoMember(14)]
        public string LoginName { get; set; }
        [ProtoMember(15)]
        public string HostName { get; set; }
        [ProtoMember(16)]
        public long? Reads { get; set; }
        [ProtoMember(17)]
        public long? Writes { get; set; }
        [ProtoMember(18)]
        public long? CPU { get; set; }      // MICROSECONDS
        [ProtoMember(19)]
        public long? Duration { get; set; } // MICROSECONDS
        [ProtoMember(20)]
        public long? EventSequence { get; set; }
        // This is the requested offset in milliseconds
        // from the the beginning of the workload
        [ProtoMember(21)]
        public double ReplayOffset { get; set; } = 0; // MILLISECONDS 
    }
}
