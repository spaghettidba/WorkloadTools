using NLog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;

namespace WorkloadTools.Consumer.WorkloadFile
{
    public class WorkloadFileWriterConsumer : BufferedWorkloadConsumer
    {

        private static Logger logger = LogManager.GetCurrentClassLogger();

        public string OutputFile { get; set; }

        private bool databaseInitialized = false;
        private int event_sequence = 1;
        private string connectionString;

        private SQLiteConnection conn;
        private SQLiteCommand events_cmd;
        private SQLiteCommand waits_cmd;
        private SQLiteCommand counters_cmd;

        private string insert_events = @"
                INSERT INTO Events (
                    event_sequence,
                    event_type,
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
                    $event_type,
                    $start_time,
                    $client_app_name,
                    $client_host_name,
                    $database_name,
                    $server_principal_name,
                    $session_id,
                    $sql_text
                );";

        private string insert_waits = @"
                INSERT INTO Waits (
                    event_sequence,
                    wait_type,
                    wait_sec,
                    resource_sec,
                    signal_sec,
                    wait_count
                )
                VALUES (
                    $event_sequence,
                    $wait_type,
                    $wait_sec,
                    $resource_sec,
                    $signal_sec,
                    $wait_count
                );";

        private string insert_counters = @"
                INSERT INTO Counters (
                    event_sequence,
                    name,
                    value
                )
                VALUES (
                    $event_sequence,
                    $name,
                    $value
                );";

        public override void ConsumeBuffered(WorkloadEvent evt)
        {
            if (!databaseInitialized)
                InitializeDatabase();

            InsertEvent(evt);
        }


        private void InsertExecutionEvent(WorkloadEvent evnt)
        {
            ExecutionWorkloadEvent evt = (ExecutionWorkloadEvent)evnt;

            if (conn == null)
            {
                conn = new SQLiteConnection(connectionString);
                conn.Open();
            }

            if (events_cmd == null)
                events_cmd = new SQLiteCommand(insert_events, conn);

            events_cmd.Parameters.AddWithValue("$event_sequence", event_sequence++);
            events_cmd.Parameters.AddWithValue("$event_type", evt.Type);
            events_cmd.Parameters.AddWithValue("$start_time", evt.StartTime);
            events_cmd.Parameters.AddWithValue("$client_app_name", evt.ApplicationName);
            events_cmd.Parameters.AddWithValue("$client_host_name", evt.HostName);
            events_cmd.Parameters.AddWithValue("$database_name", evt.DatabaseName);
            events_cmd.Parameters.AddWithValue("$server_principal_name", evt.LoginName);
            events_cmd.Parameters.AddWithValue("$session_id", evt.SPID);
            events_cmd.Parameters.AddWithValue("$sql_text", evt.Text);

            events_cmd.ExecuteNonQuery();


        }

        private void InsertEvent(WorkloadEvent evnt)
        {
            try
            {
                if ((evnt is ExecutionWorkloadEvent))
                    InsertExecutionEvent(evnt);
                if ((evnt is CounterWorkloadEvent))
                    InsertCounterEvent(evnt);
                if ((evnt is WaitStatsWorkloadEvent))
                    InsertWaitEvent(evnt);
            }
            catch (Exception e)
            {
                logger.Error(e, "Unable to write to the destination file");
                throw;
            }

        }

        private void InsertWaitEvent(WorkloadEvent evnt)
        {
            WaitStatsWorkloadEvent evt = (WaitStatsWorkloadEvent)evnt;

            if (conn == null)
            {
                conn = new SQLiteConnection(connectionString);
                conn.Open();
            }

            if (events_cmd == null)
                events_cmd = new SQLiteCommand(insert_events, conn);

            events_cmd.Parameters.AddWithValue("$event_sequence", event_sequence++);
            events_cmd.Parameters.AddWithValue("$event_type", evt.Type);
            events_cmd.Parameters.AddWithValue("$start_time", evt.StartTime);
            events_cmd.Parameters.AddWithValue("$client_app_name", null);
            events_cmd.Parameters.AddWithValue("$client_host_name", null);
            events_cmd.Parameters.AddWithValue("$database_name", null);
            events_cmd.Parameters.AddWithValue("$server_principal_name", null);
            events_cmd.Parameters.AddWithValue("$session_id", null);
            events_cmd.Parameters.AddWithValue("$sql_text", null);

            events_cmd.ExecuteNonQuery();

            if (waits_cmd == null)
                waits_cmd = new SQLiteCommand(insert_waits, conn);

            SQLiteTransaction tran = conn.BeginTransaction();
            try
            {

                foreach (DataRow dr in evt.Waits.Rows)
                {
                    waits_cmd.Parameters.AddWithValue("$event_sequence", event_sequence);
                    waits_cmd.Parameters.AddWithValue("$wait_type", dr["wait_type"]);
                    waits_cmd.Parameters.AddWithValue("$wait_sec", dr["wait_sec"]);
                    waits_cmd.Parameters.AddWithValue("$resource_sec", dr["resource_sec"]);
                    waits_cmd.Parameters.AddWithValue("$signal_sec", dr["signal_sec"]);
                    waits_cmd.Parameters.AddWithValue("$wait_count", dr["wait_count"]);

                    waits_cmd.ExecuteNonQuery();
                }

                tran.Commit();
            }
            catch (Exception)
            {
                tran.Rollback();
                throw;
            }
        }

        private void InsertCounterEvent(WorkloadEvent evnt)
        {
            CounterWorkloadEvent evt = (CounterWorkloadEvent)evnt;

            if (conn == null)
            {
                conn = new SQLiteConnection(connectionString);
                conn.Open();
            }

            if (events_cmd == null)
                events_cmd = new SQLiteCommand(insert_events, conn);

            events_cmd.Parameters.AddWithValue("$event_sequence", event_sequence++);
            events_cmd.Parameters.AddWithValue("$event_type", evt.Type);
            events_cmd.Parameters.AddWithValue("$start_time", evt.StartTime);
            events_cmd.Parameters.AddWithValue("$client_app_name", null);
            events_cmd.Parameters.AddWithValue("$client_host_name", null);
            events_cmd.Parameters.AddWithValue("$database_name", null);
            events_cmd.Parameters.AddWithValue("$server_principal_name", null);
            events_cmd.Parameters.AddWithValue("$session_id", null);
            events_cmd.Parameters.AddWithValue("$sql_text", null);

            events_cmd.ExecuteNonQuery();

            if (counters_cmd == null)
                counters_cmd = new SQLiteCommand(insert_counters, conn);

            SQLiteTransaction tran = conn.BeginTransaction();
            try
            {

                foreach (var dr in evt.Counters)
                {
                    counters_cmd.Parameters.AddWithValue("$event_sequence", event_sequence);
                    counters_cmd.Parameters.AddWithValue("$name", dr.Key.ToString());
                    counters_cmd.Parameters.AddWithValue("$value", dr.Value);


                    counters_cmd.ExecuteNonQuery();
                }

                tran.Commit();
            }
            catch (Exception)
            {
                tran.Rollback();
                throw;
            }
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
                    event_sequence INTEGER PRIMARY KEY,
                    event_type INTEGER,
                    start_time date NOT NULL,
                    client_app_name TEXT NULL, 
                    client_host_name TEXT NULL, 
                    database_name TEXT NULL, 
                    server_principal_name TEXT NULL, 
                    session_id INTEGER NULL, 
                    sql_text  TEXT NULL
                );

                CREATE TABLE IF NOT EXISTS Counters (
                    event_sequence INTEGER,
                    name TEXT NULL,
                    value FLOAT NULL
                );

                CREATE TABLE IF NOT EXISTS Waits (
                    event_sequence INTEGER,
                    wait_type TEXT NULL,
                    wait_sec INTEGER NULL,
                    resource_sec INTEGER NULL,
                    signal_sec INTEGER NULL,
                    wait_count INTEGER NULL 
                );
            ";

            string sqlMaxSeq = @"SELECT COALESCE(MAX(event_sequence),0) + 1 FROM Events;";

            connectionString = "Data Source=" + OutputFile + ";Version=3;Cache Size=10000;Locking Mode=Exclusive;";

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
                if (events_cmd != null)
                    events_cmd.Dispose();

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
