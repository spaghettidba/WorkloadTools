using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

using ProtoBuf;

namespace WorkloadTools
{
    [ProtoContract]
    public class DiskPerfWorkloadEvent : WorkloadEvent
    {
        [ProtoMember(1)]
        public DataTable DiskPerf;

        public DiskPerfWorkloadEvent()
        {
            Type = EventType.DiskPerf;
        }

    }
}
