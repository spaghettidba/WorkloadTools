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
        public IEventQueue Events { get; set; }
        public long EventCount { get; protected set; }
        public ExtendedEventsWorkloadListener.ServerType ServerType { get; set; }

        public XEventDataReader(
                string connectionString, 
                string sessionName,
                IEventQueue events,
                ExtendedEventsWorkloadListener.ServerType serverType
            )  
        {
            ConnectionString = connectionString;
            SessionName = sessionName;
            Events = events;
            ServerType = serverType;
        }



        public abstract void ReadEvents();

        public abstract void Stop();


    }
}
