using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace WorkloadTools.Listener.File
{
    public class FileEventFilter : WorkloadEventFilter
    {
        public bool IsSqlAzure { get; set; }

        public FileEventFilter()
        {
            ApplicationFilter = new FileFilterPredicate(FilterPredicate.FilterColumnName.ApplicationName);
            DatabaseFilter = new FileFilterPredicate(FilterPredicate.FilterColumnName.DatabaseName);
            HostFilter = new FileFilterPredicate(FilterPredicate.FilterColumnName.HostName);
            LoginFilter = new FileFilterPredicate(FilterPredicate.FilterColumnName.LoginName);
        }
    }
}
