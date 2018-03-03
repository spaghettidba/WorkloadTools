using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools.Listener.Trace
{
    public class TraceEventFilter : WorkloadEventFilter
    {

        public TraceEventFilter()
        {
            ApplicationFilter = new TraceFilterPredicate(FilterPredicate.FilterColumnName.ApplicationName);
            DatabaseFilter = new TraceFilterPredicate(FilterPredicate.FilterColumnName.DatabaseName);
            HostFilter = new TraceFilterPredicate(FilterPredicate.FilterColumnName.HostName);
            LoginFilter = new TraceFilterPredicate(FilterPredicate.FilterColumnName.LoginName);
        }

    }
}
