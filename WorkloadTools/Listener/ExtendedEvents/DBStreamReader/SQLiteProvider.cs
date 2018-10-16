using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;

namespace WorkloadTools.Listener.ExtendedEvents.DBStreamReader
{
    public class SQLiteProvider : IDbUtility
    {
        private readonly SQLiteConnection _currentCNS = null;
        private readonly SQLiteCommand _command;
        private int _lastId = 0;
        public SQLiteProvider()
        {
            string dbPath = Path.Combine(Environment.CurrentDirectory, "data.sqlite");
            if (System.IO.File.Exists(dbPath)) System.IO.File.Delete(dbPath);
            SQLiteConnection.CreateFile(dbPath);
            _currentCNS = new SQLiteConnection($"Data Source = {dbPath}; Version = 3; ");
            _currentCNS.Open();
            using (SQLiteCommand command = new SQLiteCommand("create table dataqueue (ID INTEGER PRIMARY KEY AUTOINCREMENT, RAWDATA VARCHAR(500), DATEREF DateTime)", _currentCNS))
            {
                command.ExecuteNonQuery();
                command.CommandText = "PRAGMA synchronous=OFF";
                command.ExecuteNonQuery();
            }

            _command = new SQLiteCommand(_currentCNS);
        }

        public (DateTime, long) GetLastInsertInfo()
        {
            using (SQLiteCommand command = new SQLiteCommand("SELECT MAX(DATEREF), COUNT(*) FROM dataqueue", _currentCNS))
            {
                using (SQLiteDataReader idr = command.ExecuteReader())
                {
                    if (idr.Read())
                    {
                        long CountAll = idr.GetInt64(1);
                        if (CountAll != 0)
                        {
                            return (idr.GetDateTime(0), CountAll);
                        }

                        return (DateTime.MinValue, CountAll);
                    }
                }
            }
            return (DateTime.MinValue, 0);
        }

        public void Save(WorkloadEvent[] objectsToSave)
        {
            using (var transaction = _currentCNS.BeginTransaction())
            {
                foreach (WorkloadEvent item in objectsToSave)
                {
                    _command.CommandText = "INSERT INTO dataqueue (RAWDATA, DATEREF) VALUES (@data, @dateref)";
                    JsonSerializerSettings settings = new JsonSerializerSettings
                    {
                        TypeNameHandling = TypeNameHandling.All
                    };
                    _command.Parameters.Add("@data", DbType.String, 20).Value = JsonConvert.SerializeObject(item, settings);
                    _command.Parameters.AddWithValue("@dateref", item.StartTime);
                    _command.ExecuteNonQuery();
                };
                transaction.Commit();
            }
        }

        public List<WorkloadEvent> Read(int toRead)
        {
            List<WorkloadEvent> result = new List<WorkloadEvent>();

            using (SQLiteCommand command = new SQLiteCommand($"SELECT ID, RAWDATA FROM dataqueue WHERE ID > {_lastId} LIMIT {toRead}", _currentCNS))
            {
                using (SQLiteDataReader idr = command.ExecuteReader())
                {
                    while (idr.Read())
                    {
                        int id = idr.GetInt32(0);
                        string data = idr.GetString(1);

                        JsonSerializerSettings settings = new JsonSerializerSettings
                        {
                            TypeNameHandling = TypeNameHandling.All
                        };
                        WorkloadEvent evt = JsonConvert.DeserializeObject<WorkloadEvent>(data, settings);
                        evt.Id = id;
                        result.Add(evt);
                    }
                }
            }
            if (result != null && result.Count > 0) _lastId = result.DefaultIfEmpty().Max(_ => _.Id);
            return result;
        }
    }
}