using NLog;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WorkloadTools;
using WorkloadTools.Listener;
using WorkloadTools.Listener.ExtendedEvents;
using WorkloadTools.Util;

namespace ConvertWorkload
{
 
    public class ExtendedEventsEventReader : EventReader
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        private string filePath;
        private bool started = false;
        private bool finished = false;

        private FileTargetXEventDataReader reader;

        public ExtendedEventsEventReader(string path)
        {
            Events = new BinarySerializedBufferedEventQueue();
            Events.BufferSize = 10000;
            filePath = path;
            Filter = new ExtendedEventsEventFilter();
        }


        private void ReadEventsFromFile()
        {
            try
            {
                SqlConnectionInfo info = new SqlConnectionInfo();
                info.ServerName = "(localdb)\\MSSQLLocalDB";

                string sqlCreateTable = @"
                    IF OBJECT_ID('tempdb.dbo.trace_reader_queue') IS NULL
                    BEGIN
                        CREATE TABLE tempdb.dbo.trace_reader_queue (
                            ts datetime DEFAULT GETDATE(),
                            path nvarchar(4000)
                        )
                    END
                    TRUNCATE TABLE tempdb.dbo.trace_reader_queue;
                    INSERT INTO tempdb.dbo.trace_reader_queue (path) VALUES(@path);
                ";

                using (SqlConnection conn = new SqlConnection())
                {
                    conn.ConnectionString = info.ConnectionString;
                    conn.Open();
                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = sqlCreateTable;
                        SqlParameter prm = new SqlParameter()
                        {
                            ParameterName = "@path",
                            DbType = System.Data.DbType.String,
                            Size = 4000,
                            Value = filePath
                        };
                        cmd.Parameters.Add(prm);
                        cmd.ExecuteNonQuery();
                    }
                }

                reader = new FileTargetXEventDataReader(info.ConnectionString, null, Events, ExtendedEventsWorkloadListener.ServerType.LocalDB);
                reader.ReadEvents();
                finished = true;
                    
            }
            catch (Exception ex)
            {
                logger.Error(ex.Message);

                if (ex.InnerException != null)
                    logger.Error(ex.InnerException.Message);

                Dispose();
            }
        }


        public override bool HasFinished()
        {
            return finished && !Events.HasMoreElements();
        }

        public override bool HasMoreElements()
        {
            return !finished && !stopped && (started ? Events.HasMoreElements() : true);
        }

        public override WorkloadEvent Read()
        {
            if (!started)
            {
                Task t = Task.Factory.StartNew(ReadEventsFromFile);
                started = true;
            }

            WorkloadEvent result = null;
            while (!Events.TryDequeue(out result))
            {
                if (stopped || finished)
                    return null;

                Thread.Sleep(5);
            }
            return result;
        }

        protected override void Dispose(bool disposing)
        {
            if (!stopped)
            {
                stopped = true;
                reader.Stop();
                reader.Dispose();
            }
        }

    }

}
