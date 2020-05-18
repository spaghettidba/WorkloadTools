using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WorkloadViewer.Model
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
        public string ApplicationName { get; set; } = "WorkloadAnalyzer";

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
                    connectionString += "Initial Catalog = " + DatabaseName + "; ";
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
                    connectionString += "Application Name = " + ApplicationName + "; ";
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
