using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools
{
    public class WorkloadEvent
    {
        public enum EventType
        {
            RPCCompleted,
            RPCStarted,
            BatchCompleted,
            Unknown
        }

        public string Text { get; set; }
        public EventType Type { get; set; } = EventType.Unknown;
        public int SPID { get; set; }
        public string ApplicationName { get; set; }
        public string DatabaseName { get; set; }
        public string LoginName { get; set; }
        public string HostName { get; set; }
        public long Reads { get; set; }
        public long Writes { get; set; }
        public long CPU { get; set; }
        public long Duration { get; set; }
        public DateTime StartTime{ get; set; }
    }
}
