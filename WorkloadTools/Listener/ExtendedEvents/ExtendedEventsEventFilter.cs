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
            ((ExtendedEventsFilterPredicate)ApplicationFilter).IsSqlAzure = this.IsSqlAzure;
            ((ExtendedEventsFilterPredicate)DatabaseFilter).IsSqlAzure = this.IsSqlAzure;
            ((ExtendedEventsFilterPredicate)HostFilter).IsSqlAzure = this.IsSqlAzure;
            ((ExtendedEventsFilterPredicate)LoginFilter).IsSqlAzure = this.IsSqlAzure;

        }
    }
}
