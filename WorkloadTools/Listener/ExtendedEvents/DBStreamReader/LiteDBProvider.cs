using LiteDB;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace WorkloadTools.Listener.ExtendedEvents.DBStreamReader
{
    public class LiteDBProvider : IDbUtility
    {
        private LiteDatabase _currentDB = null;
        private LiteCollection<WorkloadEvent> _dataCollection;
        private int _lastId = 0;

        public LiteDBProvider()
        {
            string dbPath = Path.Combine(Environment.CurrentDirectory, "events.db");
            if (System.IO.File.Exists(dbPath)) System.IO.File.Delete(dbPath);
            _currentDB = new LiteDatabase($"{dbPath}");
            _dataCollection = _currentDB.GetCollection<WorkloadEvent>("dataqueue");
            _currentDB.Mapper.Entity<WorkloadEvent>()
                            .Id(x => x.Id, true);
        }

        public void Dispose()
        {
            _currentDB.Dispose();
        }

        public (DateTime, long) GetLastInsertInfo()
        {
            _dataCollection.EnsureIndex(x => x.StartTime);
            return (_dataCollection.Max(x => x.StartTime).AsDateTime, _dataCollection.Count());
        }

        public List<WorkloadEvent> Read(int toRead)
        {
            List<WorkloadEvent> result = _dataCollection.Find(x => x.Id > _lastId).Take(toRead).ToList();
            if (result != null && result.Count > 0) _lastId = result.DefaultIfEmpty().Max(_ => _.Id);
            return result;
        }

        public void Save(WorkloadEvent[] objectToSave)
        {
            _dataCollection.InsertBulk(objectToSave, batchSize: 10000);
        }
    }
}