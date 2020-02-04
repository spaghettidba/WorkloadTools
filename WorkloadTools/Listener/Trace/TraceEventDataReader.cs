using Microsoft.SqlServer.XEvent.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools.Listener.Trace
{
    public abstract class TraceEventDataReader : IDisposable
    {

        public string ConnectionString { get; set; }
        public IEventQueue Events { get; set; }
        public long EventCount { get; protected set; }
        public WorkloadEventFilter Filter { get; set; }

        public TraceEventDataReader(
                string connectionString, 
                WorkloadEventFilter filter,
                IEventQueue events
            )  
        {
            ConnectionString = connectionString;
            Events = events;
            Filter = filter;
        }



        public abstract void ReadEvents();

        public abstract void Stop();

        public void Dispose()
        {
            Events.Dispose();
        }
    }
}
