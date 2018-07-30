using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;

namespace WorkloadTools.Consumer.WorkloadFile
{
    public class WorkloadFileWriterConsumer : BufferedWorkloadConsumer
    {
        public string OutputFile { get; set; }

        private bool databaseInitialized = false;
        private int event_sequence = 1;
        private string connectionString;

        private SQLiteConnection conn;
        private SQLiteCommand cmd;

        public override void ConsumeBuffered(WorkloadEvent evt)
        {
            if (!databaseInitialized)
                InitializeDatabase();

            InsertEvent(evt);
        }



        private void InsertEvent(WorkloadEvent evnt)
        {
            if (!(evnt is ExecutionWorkloadEvent))
                return;

            ExecutionWorkloadEvent evt = (ExecutionWorkloadEvent)evnt;

            string sql = @"
                INSERT INTO Events (
                    event_sequence,
                    start_time,
                    client_app_name,
                    client_host_name,
                    database_name,
                    server_principal_name,
                    session_id,
                    sql_text
                )
                VALUES (
                    $event_sequence,
                    $start_time,
                    $client_app_name,
                    $client_host_name,
                    $database_name,
                    $server_principal_name,
                    $session_id,
                    $sql_text
                );";

            if (conn == null)
            {
                conn = new SQLiteConnection(connectionString);
                conn.Open();
            }
                
            if(cmd == null)
                cmd = new SQLiteCommand(sql, conn);

            cmd.Parameters.AddWithValue("$event_sequence", event_sequence++);
            cmd.Parameters.AddWithValue("$start_time", evt.StartTime);
            cmd.Parameters.AddWithValue("$client_app_name", evt.ApplicationName);
            cmd.Parameters.AddWithValue("$client_host_name", evt.HostName);
            cmd.Parameters.AddWithValue("$database_name", evt.DatabaseName);
            cmd.Parameters.AddWithValue("$server_principal_name", evt.LoginName);
            cmd.Parameters.AddWithValue("$session_id", evt.SPID);
            cmd.Parameters.AddWithValue("$sql_text", evt.Text);

            cmd.ExecuteNonQuery();
            
        }


        public void InitializeDatabase()
        {
            if (!File.Exists(OutputFile))
            {
                Directory.CreateDirectory(Directory.GetParent(OutputFile).FullName);
                SQLiteConnection.CreateFile(OutputFile);
            }

            string sqlCreateTable = @"
                CREATE TABLE IF NOT EXISTS Events (
                    event_sequence INTEGER,
                    start_time date NOT NULL,
                    client_app_name TEXT NULL, 
                    client_host_name TEXT NULL, 
                    database_name TEXT NULL, 
                    server_principal_name TEXT NULL, 
                    session_id INTEGER NOT NULL, 
                    sql_text  TEXT NULL
                )
            ";

            string sqlMaxSeq = @"SELECT COALESCE(MAX(event_sequence),0) + 1 FROM Events;";

            connectionString = "Data Source=" + OutputFile + ";Version=3;";

            using (SQLiteConnection m_dbConnection = new SQLiteConnection(connectionString))
            {
                m_dbConnection.Open();
                try
                {
                    SQLiteCommand command = new SQLiteCommand(sqlCreateTable, m_dbConnection);
                    command.ExecuteNonQuery();

                    command = new SQLiteCommand(sqlMaxSeq, m_dbConnection);
                    event_sequence = Convert.ToInt32(command.ExecuteScalar());
                }
                catch (Exception)
                {
                    throw;
                }
            }

            databaseInitialized = true;

        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                if (cmd != null)
                    cmd.Dispose();

                if (conn != null)
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
            catch(Exception)
            {
                //ignore
            }

            stopped = true;
        }
    }
}
