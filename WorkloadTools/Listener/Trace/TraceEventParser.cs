using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadTools.Listener.Trace
{
    public class TraceEventParser
    {
        public enum EventClassEnum : int
        {
            RPC_Completed = 10,
            SQL_BatchCompleted = 12,
            RPC_Starting = 11,
            SQL_BatchStarting = 13,
            Audit_Login = 14,
            Timeout = 82
        }

        private Dictionary<string, string> columns = new Dictionary<string, string>();


        public ExecutionWorkloadEvent ParseEvent(SqlDataReader reader)
        {
            if(columns.Count == 0)
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    string colName = reader.GetName(i);
                    columns.Add(colName.ToLower(), colName);
                }
            }

            ExecutionWorkloadEvent evt = new ExecutionWorkloadEvent();

            int eventClass = (int)reader["EventClass"];


            if (eventClass == (int)EventClassEnum.RPC_Starting)
                evt.Type = WorkloadEvent.EventType.RPCStarting;
            else if (eventClass == (int)EventClassEnum.SQL_BatchStarting)
                evt.Type = WorkloadEvent.EventType.BatchStarting;
            else if (eventClass == (int)EventClassEnum.RPC_Completed)
                evt.Type = WorkloadEvent.EventType.RPCCompleted;
            else if (eventClass == (int)EventClassEnum.SQL_BatchCompleted)
                evt.Type = WorkloadEvent.EventType.BatchCompleted;
            else if (eventClass == (int)EventClassEnum.Audit_Login)
            {
                if (IsValidColumn("EventSubClass") && reader["EventSubClass"] != DBNull.Value)
                {
                    int vEventSubClass = (int)(reader["EventSubClass"]);
                    if (vEventSubClass == 1) /* 1 - Nonpooled */
                    { 
                        evt.Type = WorkloadEvent.EventType.RPCStarting;
                        // A nonpooled login will trigger Login event with EventSubClass = 1
                        // Setting text to sp_reset_connection and including comment on to 
                        // be able to understand this is a nonpooled login on replay
                        evt.Text = "exec sp_reset_connection /*Nonpooled*/";
                    }
                    else
                    {
                        evt.Type = WorkloadEvent.EventType.Unknown;
                        return evt;
                    }
                }
            }
            else if (eventClass == (int)EventClassEnum.Timeout)
            {
                if (reader["TextData"].ToString().StartsWith("WorkloadTools.Timeout["))
                    evt.Type = WorkloadEvent.EventType.Timeout;
            }
            else
            {
                evt.Type = WorkloadEvent.EventType.Unknown;
                return evt;
            }
            if (IsValidColumn("ApplicationName") && reader["ApplicationName"] != DBNull.Value)
                evt.ApplicationName = (string)reader["ApplicationName"];
            if (IsValidColumn("DatabaseName") && reader["DatabaseName"] != DBNull.Value)
                evt.DatabaseName = (string)reader["DatabaseName"];
            if (IsValidColumn("Hostname") && reader["HostName"] != DBNull.Value)
                evt.HostName = (string)reader["HostName"];
            if (IsValidColumn("LoginName") && reader["LoginName"] != DBNull.Value)
                evt.LoginName = (string)reader["LoginName"];
            if (IsValidColumn("SPID") && reader["SPID"] != DBNull.Value)
                evt.SPID = (int?)reader["SPID"];
            if (IsValidColumn("TextData") && reader["TextData"] != DBNull.Value && eventClass != (int)EventClassEnum.Audit_Login)
                evt.Text = (string)reader["TextData"];

            if (IsValidColumn("StartTime") && reader["StartTime"] != DBNull.Value)
                evt.StartTime = (DateTime)reader["StartTime"];

            if (evt.Type == WorkloadEvent.EventType.Timeout)
            {
                if (IsValidColumn("BinaryData") && reader["BinaryData"] != DBNull.Value)
                {
                    byte[] bytes = (byte[])reader["BinaryData"];
                    evt.Text = Encoding.Unicode.GetString(bytes);
                }
                if(IsValidColumn("TextData") && reader["TextData"] != DBNull.Value)
                    evt.Duration = ExtractTimeoutDuration(reader["TextData"]);
                evt.CPU = Convert.ToInt64(evt.Duration);
            }
            else
            {
                if (IsValidColumn("Reads") && reader["Reads"] != DBNull.Value)
                    evt.Reads = (long?)reader["Reads"];
                if (IsValidColumn("Writes") && reader["Writes"] != DBNull.Value)
                    evt.Writes = (long?)reader["Writes"];
                if (IsValidColumn("CPU") && reader["CPU"] != DBNull.Value)
                    evt.CPU = (long?)Convert.ToInt64(reader["CPU"]) * 1000; // SqlTrace captures CPU as milliseconds => convert to microseconds
                if (IsValidColumn("Duration") && reader["Duration"] != DBNull.Value)
                    evt.Duration = (long?)reader["Duration"];
                if (IsValidColumn("EventSequence") && reader["EventSequence"] != DBNull.Value)
                    evt.EventSequence = (long?)reader["EventSequence"];
            }

            return evt;
        }

        private long? ExtractTimeoutDuration(object textData)
        {
            long result = 30;
            if (textData != DBNull.Value)
            {
                string description = (string)textData;
                string durationAsString = new String(description.Where(Char.IsDigit).ToArray());
                result = Convert.ToInt64(durationAsString);
            }
            return result * 1000 * 1000;
        }

        private bool IsValidColumn(string colName)
        {
            return columns.ContainsKey(colName.ToLower());
        }
    }
}
