using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadTools.Listener.Trace
{
    internal class TraceUtils
    {
        public int GetTraceId(SqlConnection conn, string path)
        {
            string sql = @"
                SELECT TOP(1) id
                FROM (
	                SELECT id FROM sys.traces WHERE path LIKE '{0}%'
	                UNION ALL
	                SELECT -1
                ) AS i
                ORDER BY id DESC
            ";

            SqlCommand cmd = conn.CreateCommand();
            cmd.CommandText = String.Format(sql, path);
            return (int)cmd.ExecuteScalar();
        }
    }
}
