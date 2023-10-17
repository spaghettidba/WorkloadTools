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
            var sql = @"
                SELECT TOP(1) id
                FROM (
	                SELECT id FROM sys.traces WHERE path LIKE '{0}%'
	                UNION ALL
	                SELECT -1
                ) AS i
                ORDER BY id DESC
            ";

            var cmd = conn.CreateCommand();
            cmd.CommandText = string.Format(sql, path);
            return (int)cmd.ExecuteScalar();
        }

        public string GetSqlDefaultLogPath(SqlConnection conn)
        {
            var sql = @"
            DECLARE @defaultLog nvarchar(4000);

            EXEC master.dbo.xp_instance_regread
	            N'HKEY_LOCAL_MACHINE',
	            N'Software\Microsoft\MSSQLServer\MSSQLServer',
	            N'DefaultLog',
	            @defaultLog OUTPUT;

            IF @defaultLog IS NULL
            BEGIN
	            SELECT @defaultLog = REPLACE(physical_name,'mastlog.ldf','') 
	            FROM sys.master_files
                WHERE file_id = 2
					AND database_id = 1;
            END

            SELECT @defaultLog AS DefaultLog;
        ";
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = sql;
                return (string)cmd.ExecuteScalar();
            }
        }

        
        public bool CheckTraceFormat(SqlConnection conn, string path)
        {
            var sql = @"
                SELECT COUNT(*) AS cnt
                FROM(
                    SELECT TOP(100) *
                    FROM fn_trace_gettable(@path, default)
                ) AS data
                WHERE EventSequence IS NOT NULL
                    AND SPID IS NOT NULL
            ";
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = sql;
                var p = cmd.CreateParameter();
                p.ParameterName = "@path";
                p.DbType = System.Data.DbType.AnsiString;
                p.Value = path;
                _ = cmd.Parameters.Add(p);
                return ((int)cmd.ExecuteScalar()) > 0;
            }

        }
    }
}
