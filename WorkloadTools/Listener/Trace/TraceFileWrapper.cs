using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace WorkloadTools.Listener.Trace
{
    public class TraceFileWrapper : IDisposable
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private static readonly Assembly _baseAssembly;
        private static readonly Type _baseType;

        static TraceFileWrapper()
        {
            try
            {
#pragma warning disable 618
                _baseAssembly = Assembly.LoadWithPartialName("Microsoft.SqlServer.ConnectionInfoExtended");
#pragma warning restore 618
                logger.Info(string.Format("SMO Version: {0}", (object)_baseAssembly.FullName.ToString()));
                _baseType = _baseAssembly.GetType("Microsoft.SqlServer.Management.Trace.TraceFile");
            }
            catch (Exception ex)
            {
                logger.Error("Unable to load SMO library");
                logger.Error(ex.Message);
                throw;
            }
        }

        public object TraceFile { get; set; }

        public TraceFileWrapper()
        {
            TraceFile = _baseType.InvokeMember((string)null, BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.CreateInstance, (Binder)null, (object)null, (object[])null);
        }

        public object this[string name]
        {
            get
            {
                var indexer = _baseType
                    .GetProperties()
                    .Single(p => p.GetIndexParameters().Length == 1 && p.GetIndexParameters()[0].ParameterType == typeof(string));
                return indexer.GetValue(TraceFile, new object[] { name });
            }
        }

        public object this[int index]
        {
            get
            {
                var indexer = _baseType
                    .GetProperties()
                    .Single(p => p.GetIndexParameters().Length == 1 && p.GetIndexParameters()[0].ParameterType == typeof(int));
                return indexer.GetValue(TraceFile, new object[] { index });
            }
        }

        public object GetValue(string Name)
        {
            return this[Name];
        }

        public bool HasAttribute(string Name)
        {
            try
            {
                _ = GetValue(Name);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public void InitializeAsReader(string fileName)
        {
            try
            {
                var args = new object[1] { fileName };
                _ = _baseType.InvokeMember("InitializeAsReader", BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.InvokeMethod, (Binder)null, TraceFile, args);
            }
            catch (Exception)
            {
                throw;
            }
        }

        public bool Read()
        {
            return (bool)_baseType.InvokeMember("Read", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.InvokeMethod, (Binder)null, TraceFile, (object[])null);
        }

        public void Dispose()
        {
            // naughty, naughty...
            return;
        }
    }
}
