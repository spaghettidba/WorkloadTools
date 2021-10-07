using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools.Consumer.Replay
{
    public class ReplayCommand
    {
        public string CommandText { get; set; }
        public string Database { get; set; }
        public string ApplicationName { get; set; }
        public long ReplayOffset { get; set; } = 0; // milliseconds
        public long? EventSequence { get; set; }
    }
}
