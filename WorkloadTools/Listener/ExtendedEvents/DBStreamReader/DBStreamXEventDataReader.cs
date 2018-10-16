using Microsoft.SqlServer.XEvent.Linq;
using NLog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools.Listener.ExtendedEvents.DBStreamReader
{
    public class DBStreamXEventDataReader : StreamXEventDataReader 
    {
        private List<WorkloadEvent> _localData = new List<WorkloadEvent>();
        private ConcurrentQueue<WorkloadEvent> _readedLocalData = new ConcurrentQueue<WorkloadEvent>();

        public DBStreamXEventDataReader(string connectionString, string sessionName, ConcurrentQueue<WorkloadEvent> events) : base(connectionString, sessionName, events)
        {
        }

        public override void SaveEvent(WorkloadEvent evnt)
        {
            _localData.Add(evnt);
            if (_localData.Count > 0 && (_localData[_localData.Count - 1].StartTime - _localData[0].StartTime).TotalMilliseconds > 500)
            {
                DatabaseFactory.Current.Save(_localData.ToArray());
                _localData.Clear();
            }            
        }

        public override WorkloadEvent Read()
        {
            if(_readedLocalData.Count == 0)
            {
                DatabaseFactory.Current
                                .Read(10000)
                                .ForEach(o => _readedLocalData.Enqueue(o));
            }
            if (_readedLocalData.Count > 0)
            {
                _readedLocalData.TryDequeue(out WorkloadEvent result);
                return result;
            }
            else return null;
        }
    }
}
