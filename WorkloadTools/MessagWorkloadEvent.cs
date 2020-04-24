using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadTools
{
    [Serializable]
    public class MessageWorkloadEvent : WorkloadEvent
    {
        public enum MessageType
        {
            TotalEvents
        }

        public MessageType MsgType { get; set; }
        public object Value { get; set; }

        public MessageWorkloadEvent()
        {
            Type = EventType.Message;
        }
    }
}
