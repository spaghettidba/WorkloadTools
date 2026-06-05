using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using ProtoBuf;

namespace WorkloadTools
{
    [ProtoContract]
    public class ExecutionWorkloadEvent : WorkloadEvent
    {
        [ProtoMember(1)]
        public string Text { get; set; }
        [ProtoMember(2)]
        public int? SPID { get; set; }
        [ProtoMember(3)]
        public string ApplicationName { get; set; }
        [ProtoMember(4)]
        public string DatabaseName { get; set; }
        [ProtoMember(5)]
        public string LoginName { get; set; }
        [ProtoMember(6)]
        public string HostName { get; set; }
        [ProtoMember(7)]
        public long? Reads { get; set; }
        [ProtoMember(8)]
        public long? Writes { get; set; }
        [ProtoMember(9)]
        public long? CPU { get; set; }      // MICROSECONDS
        [ProtoMember(10)]
        public long? Duration { get; set; } // MICROSECONDS
        [ProtoMember(11)]
        public long? EventSequence { get; set; }
        // This is the requested offset in milliseconds
        // from the the beginning of the workload
        [ProtoMember(12)]
        public double ReplayOffset { get; set; } = 0; // MILLISECONDS 
    }
}
