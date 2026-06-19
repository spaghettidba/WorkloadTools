using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

using Microsoft.Data.Sqlite;
using DuckDB.NET.Data;

namespace WorkloadTools
{
    public class SqlConnectionInfo
    {
        public enum DatabaseTypeEnum
        {
            SqlServer,
            Sqlite,
            DuckDB
        }

        public string DatabaseType { get; set; } = DatabaseTypeEnum.SqlServer.ToString();
        public string ServerName { get; set; }
        public string DataSource { get => ServerName; set => ServerName = value; }
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
            if(DatabaseType == DatabaseTypeEnum.Sqlite.ToString())
            {
                var builder = new SqliteConnectionStringBuilder()
                {
                    DataSource = DataSource,
                    Mode = SqliteOpenMode.ReadWriteCreate,
                    Cache = SqliteCacheMode.Default
                };
                return builder.ConnectionString;
            }
            else if(DatabaseType == DatabaseTypeEnum.DuckDB.ToString())
            {
                var builder = new DuckDBConnectionStringBuilder()
                {
                    DataSource = DataSource,
                };
                return builder.ConnectionString;
            }
            else if (DatabaseType == DatabaseTypeEnum.SqlServer.ToString())
            {
                if(string.IsNullOrEmpty(UserName) || string.IsNullOrEmpty(Password))
                {
                    UseIntegratedSecurity = true;
                    UserName = "";
                    Password = "";
                }
                var builder = new SqlConnectionStringBuilder()
                {
                    DataSource = ServerName,
                    MaxPoolSize = MaxPoolSize,
                    InitialCatalog = string.IsNullOrEmpty(DatabaseName) ? "master" : (DatabaseMap.ContainsKey(DatabaseName) ? DatabaseMap[DatabaseName] : DatabaseName),
                    IntegratedSecurity = UseIntegratedSecurity,
                    UserID = UserName,
                    Password = Password,
                    ApplicationName = applicationName,
                    Encrypt = Encrypt,
                    TrustServerCertificate = TrustServerCertificate
                };
                return builder.ConnectionString;
            }
            else
            {
                throw new NotSupportedException("Unsupported database type: " + DatabaseType);
            }
        }
    }
}
