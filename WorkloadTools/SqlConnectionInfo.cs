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
        public int MaxPoolSize { get; set; } = 500;
        public Dictionary<string, string> DatabaseMap { get; set; } = new Dictionary<string, string>();


        public SqlConnectionInfo() { }

        public SqlConnectionInfo(SqlConnectionInfo info)
        {
            this.ServerName = info.ServerName;
            this.DatabaseName = info.DatabaseName;
            this.SchemaName = info.SchemaName;
            this.UseIntegratedSecurity = info.UseIntegratedSecurity;
            this.UserName = info.UserName;
            this.Password = info.Password;
            this.Encrypt = info.Encrypt;
            this.TrustServerCertificate = info.TrustServerCertificate;
            this.ApplicationName = info.ApplicationName;
            this.MaxPoolSize = info.MaxPoolSize;
            this.DatabaseMap = info.DatabaseMap;
        }

        public string ConnectionString()
        {
            return ConnectionString(ApplicationName);
        }

        public string ConnectionString(string applicationName)
        {
            var connectionString = "Data Source=" + ServerName + "; ";
            connectionString += "Max Pool Size = " + MaxPoolSize + "; ";
            if (string.IsNullOrEmpty(DatabaseName))
            {
                connectionString += "Initial Catalog = master; ";
            }
            else
            {
                // try to replace database name with the name
                // in the database map, if any
                var effectiveDatabaseName = DatabaseName;
                if (DatabaseMap.ContainsKey(DatabaseName))
                {
                    effectiveDatabaseName = DatabaseMap[DatabaseName];
                }
                connectionString += "Initial Catalog = " + effectiveDatabaseName + "; ";
            }
            if (string.IsNullOrEmpty(UserName))
            {
                connectionString += "Integrated Security = SSPI; ";
            }
            else
            {
                connectionString += "User Id = " + UserName + "; ";
                connectionString += "Password = " + Password + "; ";
            }
            if (!string.IsNullOrEmpty(applicationName))
            {
                connectionString += "Application Name = " + applicationName + "; ";
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
