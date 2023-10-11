using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace WorkloadTools.Listener.Trace
{
    public class SqlConnectionInfoWrapper
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public object SqlConnectionInfo { get; set; }
        public string ServerName
        {
            get
            {
                return (string)SqlConnectionInfo.GetType().GetProperty("ServerName")?.GetGetMethod()?.Invoke(SqlConnectionInfo, (object[])null);
            }
            set
            {
                _ = (SqlConnectionInfo.GetType().GetProperty("ServerName")?.GetSetMethod()?.Invoke(SqlConnectionInfo, new object[] { value }));
            }
        }
        public string DatabaseName
        {
            get
            {
                return (string)SqlConnectionInfo.GetType().GetProperty("DatabaseName")?.GetGetMethod()?.Invoke(SqlConnectionInfo, (object[])null);
            }
            set
            {
                _ = (SqlConnectionInfo.GetType().GetProperty("DatabaseName")?.GetSetMethod()?.Invoke(SqlConnectionInfo, new object[] { value }));
            }
        }
        public bool UseIntegratedSecurity
        {
            get
            {
                return (bool)SqlConnectionInfo.GetType().GetProperty("UseIntegratedSecurity")?.GetGetMethod()?.Invoke(SqlConnectionInfo, (object[])null);
            }
            set
            {
                _ = (SqlConnectionInfo.GetType().GetProperty("UseIntegratedSecurity")?.GetSetMethod()?.Invoke(SqlConnectionInfo, new object[] { value }));
            }
        }
        public string UserName
        {
            get
            {
                return (string)SqlConnectionInfo.GetType().GetProperty("UserName")?.GetGetMethod()?.Invoke(SqlConnectionInfo, (object[])null);
            }
            set
            {
                _ = (SqlConnectionInfo.GetType().GetProperty("UserName")?.GetSetMethod()?.Invoke(SqlConnectionInfo, new object[] { value }));
            }
        }
        public string Password
        {
            get
            {
                return (string)SqlConnectionInfo.GetType().GetProperty("Password")?.GetGetMethod()?.Invoke(SqlConnectionInfo, (object[])null);
            }
            set
            {
                _ = (SqlConnectionInfo.GetType().GetProperty("Password")?.GetSetMethod()?.Invoke(SqlConnectionInfo, new object[] { value }));
            }
        }

        public SqlConnectionInfoWrapper()
        {
            Type type;
            try
            {
#pragma warning disable 618
                var assembly = Assembly.LoadWithPartialName("Microsoft.SqlServer.ConnectionInfo");
#pragma warning restore 618
                type = assembly.GetType("Microsoft.SqlServer.Management.Common.SqlConnectionInfo");
                SqlConnectionInfo = type.InvokeMember((string)null, BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.CreateInstance, (Binder)null, (object)null, (object[])null);
            }
            catch (Exception ex)
            {
                logger.Error("Unable to load SMO library");
                logger.Error(ex.Message);
                throw;
            }
        }

    }
}
