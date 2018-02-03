using NLog;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace WorkloadTools.Listener.Trace
{
    public class TraceServerWrapper
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        private static Assembly _baseAssembly;
        private static Type _baseType;

        private bool isRunning = false;
        public bool IsRunning { get { return isRunning; } }

        static TraceServerWrapper()
        {
            try
            {
                _baseAssembly = Assembly.LoadWithPartialName("Microsoft.SqlServer.ConnectionInfoExtended");
                logger.Info(string.Format("SMO Version: {0}", (object)_baseAssembly.FullName.ToString()));
                _baseType = _baseAssembly.GetType("Microsoft.SqlServer.Management.Trace.TraceServer");
            }
            catch (Exception ex)
            {
                logger.Error("Unable to load SMO library");
                logger.Error(ex.Message);
                throw;
            }
        }

        public TraceServerWrapper()
        {
            TraceServer = _baseType.InvokeMember((string)null, BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.CreateInstance, (Binder)null, (object)null, (object[])null);
        }

        public object TraceServer { get; set; }


        public object this[
            string name
        ]
        {
            get
            {
                PropertyInfo indexer = _baseType
                    .GetProperties()
                    .Single(p => p.GetIndexParameters().Length == 1 && p.GetIndexParameters()[0].ParameterType == typeof(string));
                //PropertyInfo indexer = _baseType.GetProperty("Item");
                return indexer.GetValue(TraceServer, new object[] { name });
                //return _baseType.InvokeMember("get_Item", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.InvokeMethod, (Binder)null, TraceServer, new object[] { name });
            }
        }

        public object GetValue(string Name)
        {
            return this[Name];
        }

        public bool Read()
        {
            return (bool)_baseType.InvokeMember("Read", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.InvokeMethod, (Binder)null, TraceServer, (object[])null);
        }

        public void Stop()
        {
            //_baseType.InvokeMember("Pause", BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.InvokeMethod, (Binder)null, TraceServer, (object[])null);
            isRunning = false;
            _baseType.InvokeMember("Stop", BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.InvokeMethod, (Binder)null, TraceServer, (object[])null);
        }

        public void Close()
        {
            _baseType.InvokeMember("Close", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.InvokeMethod, (Binder)null, TraceServer, (object[])null);
        }

        public void InitializeAsReader(SqlConnectionInfoWrapper connectionInfo, string TraceDefinition)
        {
            try
            {
                object[] args = new object[2] { connectionInfo.SqlConnectionInfo, TraceDefinition };
                _baseType.InvokeMember("InitializeAsReader", BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.InvokeMethod, (Binder)null, TraceServer, args);
                isRunning = true;
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}
