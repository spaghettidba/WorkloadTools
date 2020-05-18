using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools
{
    public class SqlConnectionInfo
    {
        public string ServerName { get; set; }
        public string DatabaseName { get; set; } = "master";
        public string SchemaName { get; set; } = "dbo";
        public bool UseIntegratedSecurity { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }
        public bool Encrypt { get; set; } = false;
        public bool TrustServerCertificate { get; set; } = false;
        public string ApplicationName { get; set; } = "WorkloadTools";
        public Dictionary<string, string> DatabaseMap { get; set;} = new Dictionary<string, string>();
            
        public string ConnectionString
        {
            get
            {
                string connectionString = "Data Source=" + ServerName + ";";
                if (String.IsNullOrEmpty(DatabaseName))
                {
                    connectionString += "Initial Catalog = master; ";
                }
                else
                {
                    // try to replace database name with the name
                    // in the database map, if any
                    string effectiveDatabaseName = DatabaseName;
                    if (DatabaseMap.ContainsKey(DatabaseName))
                    {
                        effectiveDatabaseName = DatabaseMap[DatabaseName];
                    }
                    connectionString += "Initial Catalog = " + effectiveDatabaseName + "; ";
                }
                if (String.IsNullOrEmpty(UserName))
                {
                    connectionString += "Integrated Security = SSPI; ";
                }
                else
                {
                    connectionString += "User Id = " + UserName + "; ";
                    connectionString += "Password = " + Password + "; ";
                }
                if (!String.IsNullOrEmpty(ApplicationName))
                {
                    connectionString += "Application Name = "+ ApplicationName +"; ";
                }
                if (Encrypt)
                {
                    connectionString += "Encrypt = true; ";
                }
                if (TrustServerCertificate)
                {
                    connectionString += "TrustServerCertificate = true; ";
                }
                return connectionString;
            }
        }
    }
}
