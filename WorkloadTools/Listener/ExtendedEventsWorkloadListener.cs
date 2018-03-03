using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools.Listener
{
    public class ExtendedEventsWorkloadListener : WorkloadListener
    {
        private static Logger logger = LogManager.GetCurrentClassLogger();

        public override void Initialize()
        {
            throw new NotImplementedException();
            // Push Down EventFilters
            //traceSql += Environment.NewLine + Filter.ApplicationFilter.PushDown();
            //traceSql += Environment.NewLine + Filter.DatabaseFilter.PushDown();
            //traceSql += Environment.NewLine + Filter.HostFilter.PushDown();
            //traceSql += Environment.NewLine + Filter.LoginFilter.PushDown();
        }

        public override WorkloadEvent Read()
        {
            throw new NotImplementedException();
        }

        protected override void Dispose(bool disposing)
        {
            throw new NotImplementedException();
        }
    }
}
