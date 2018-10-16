using Microsoft.SqlServer.XEvent.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace WorkloadTools.Listener.ExtendedEvents
{
    public abstract class XEventDataReader : IDisposable
    {
        public string ConnectionString { get; set; }
        public string SessionName { get; set; }
        public ConcurrentQueue<WorkloadEvent> Events { get; set; }
        public long EventCount { get; set; }
        protected bool Stopped { get; set; }
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

        public virtual WorkloadEvent Read()
        {
            WorkloadEvent result = null;
            while (!Events.TryDequeue(out result))
            {
                if (Stopped)
                    return null;

                Thread.Sleep(5);
            }
            return result;
        }

        public virtual void SaveEvent(WorkloadEvent evnt)
        {
            Events.Enqueue(evnt);
        }

        public void Dispose()
        {
            Stopped = true;
        }
    }
}
