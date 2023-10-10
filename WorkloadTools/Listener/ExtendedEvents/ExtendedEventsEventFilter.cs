using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools.Listener.ExtendedEvents
{
    public class ExtendedEventsEventFilter : WorkloadEventFilter
    {
        public bool IsSqlAzure { get; set; }

        public ExtendedEventsEventFilter()
        {
            ApplicationFilter = new ExtendedEventsFilterPredicate(FilterPredicate.FilterColumnName.ApplicationName);
            DatabaseFilter = new ExtendedEventsFilterPredicate(FilterPredicate.FilterColumnName.DatabaseName);
            HostFilter = new ExtendedEventsFilterPredicate(FilterPredicate.FilterColumnName.HostName);
            LoginFilter = new ExtendedEventsFilterPredicate(FilterPredicate.FilterColumnName.LoginName);
            ((ExtendedEventsFilterPredicate)ApplicationFilter).IsSqlAzure = IsSqlAzure;
            ((ExtendedEventsFilterPredicate)DatabaseFilter).IsSqlAzure = IsSqlAzure;
            ((ExtendedEventsFilterPredicate)HostFilter).IsSqlAzure = IsSqlAzure;
            ((ExtendedEventsFilterPredicate)LoginFilter).IsSqlAzure = IsSqlAzure;

        }
    }
}
