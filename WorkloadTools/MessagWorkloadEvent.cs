using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ProtoBuf;

namespace WorkloadTools
{
    [ProtoContract]
    public class MessageWorkloadEvent : WorkloadEvent
    {
        public enum MessageType
        {
            TotalEvents
        }

        [ProtoMember(10)]
        public MessageType MsgType { get; set; }

        [ProtoMember(11)]
        public long Value { get; set; }

        public MessageWorkloadEvent()
        {
            Type = EventType.Message;
        }
    }
}
