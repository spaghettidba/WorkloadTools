using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools
{
    public class SqlConnectionInfo
    {
        public string ServerName { get; set; }
        public string DatabaseName { get; set; }
        public string SchemaName { get; set; }
        public bool UseIntegratedSecurity { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }

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
                return connectionString;
            }
        }
    }
}
