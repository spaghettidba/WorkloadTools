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
        }

        public override WorkloadEvent Read()
        {
            throw new NotImplementedException();
        }
    }
}
