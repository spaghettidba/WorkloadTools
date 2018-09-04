using Microsoft.SqlServer.XEvent.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools.Listener.ExtendedEvents
{
    public abstract class XEventDataReader 
    {

        public string ConnectionString { get; set; }
        public string SessionName { get; set; }
        public ConcurrentQueue<WorkloadEvent> Events { get; set; }
        public long EventCount { get; set; }

        public XEventDataReader(
                string connectionString, 
                string sessionName, 
                ConcurrentQueue<WorkloadEvent> events
            )  
        {
            ConnectionString = connectionString;
            SessionName = sessionName;
            Events = events;
        }



        public abstract void ReadEvents();

        public abstract void Stop();


    }
}
