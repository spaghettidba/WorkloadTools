using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools.Listener.ExtendedEvents.DBStreamReader
{
    public interface IDbUtility
    {
        void Save(WorkloadEvent[] objectToSave);
        (DateTime, long) GetLastInsertInfo();
        
        List<WorkloadEvent> Read(int toRead);
    }

    
}
