using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools.Listener.Trace
{
    public class ProfilerEventFilter : WorkloadEventFilter
    {

        public ProfilerEventFilter()
        {
            ApplicationFilter = new ProfilerFilterPredicate(FilterPredicate.FilterColumnName.ApplicationName);
            DatabaseFilter = new ProfilerFilterPredicate(FilterPredicate.FilterColumnName.DatabaseName);
            HostFilter = new ProfilerFilterPredicate(FilterPredicate.FilterColumnName.HostName);
            LoginFilter = new ProfilerFilterPredicate(FilterPredicate.FilterColumnName.LoginName);
        }

    }
}
