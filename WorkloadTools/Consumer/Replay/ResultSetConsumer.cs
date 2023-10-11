using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace WorkloadTools.Consumer.Replay
{
    public class ResultSetConsumer : IDisposable
    {
        private readonly SqlDataReader reader;

        public ResultSetConsumer(SqlDataReader sqlDataReader)
        {
            reader = sqlDataReader;
        }

        public void Consume()
        {
            while (reader.Read())
            {
                // do nothing: I just need to pull all the rows
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing) { }

    }
}
